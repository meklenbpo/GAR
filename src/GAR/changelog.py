"""
GAR
===

Changelog
---------

Compute a change log between two GAR files.
"""

import os

import pandas as pd


def disassemble_file(fias_fn: str, parts_dir: str) -> None:
    """Split a FIAS CSV file into parts."""
    print(f'Disassembling {fias_fn}:')
    cols = [
        'guid', 'current', 'postalcode', 'housenum', 'buildnum', 'strucnum',
        'street_s', 'street_f', 'terr_s', 'terr_f', 'place_s', 'place_f',
        'city_s', 'city_f', 'muni_s', 'muni_f', 'munr_s', 'munr_f',
        'region_s', 'region_f'
    ]
    addr_cols = [x for x in cols if x not in ['guid', 'current', 'postalcode']]
    attrs = {
        'filepath_or_buffer': fias_fn,
        'sep': '¬',
        'engine': 'python',
        'usecols': cols,
        'chunksize': 10**6,
        'dtype': 'str'
    }
    os.makedirs(parts_dir, exist_ok=True)
    pr = pd.read_csv(**attrs)
    all_hex = '0123456789abcdef'
    for bidx, batch in enumerate(pr):
        print(bidx, end=' ', flush=True)
        b = batch.fillna('')
        b['addr'] = ''
        b.addr = b.fillna('').addr.str.cat(b[addr_cols], sep='-').str[1:]
        b.drop(addr_cols, axis=1, inplace=True)
        for hex_prefix in all_hex:
            p = b.loc[b.guid.str.startswith(hex_prefix)]
            fn = f'{hex_prefix}_{str(bidx).zfill(2)}.fea'
            p.reset_index(drop=True).to_feather(os.path.join(parts_dir, fn))
    print('Done.')


def assemble_by_prefix(parts_dir: str, save_dir: str) -> None:
    """Assemble FIAS parts by prefix."""
    print(f'Re-assembling from {parts_dir} to {save_dir}:')
    os.makedirs(save_dir, exist_ok=True)
    fl = os.listdir(parts_dir)
    fl = sorted([x for x in fl if x.endswith('.fea')])
    all_hex = '0123456789abcdef'
    for hex_prefix in all_hex:
        fl_hex = [x for x in fl if x.startswith(hex_prefix)]
        fl_full = [os.path.join(parts_dir, x) for x in fl_hex]
        data = [pd.read_feather(x) for x in fl_full]
        cdata = pd.concat(data, ignore_index=True)
        filename = os.path.join(save_dir, f'{hex_prefix}.fea')
        cdata.to_feather(filename)
        print(hex_prefix, end=' ', flush=True)
    print('Done.')


def compare_two_prefixes(fn1: str, fn2: str) -> pd.DataFrame:
    """Merge two datasets together and add status to each line: either
    it is a new address, this address was deleted or the address has
    been changed."""
    df1 = pd.read_feather(fn1)
    df2 = pd.read_feather(fn2)
    dfm = pd.merge(df1, df2, how='outer', on=['guid', 'current', 'postalcode'])
    dfm['status'] = ''
    dfm.loc[dfm.addr_x.isna(), 'status'] = 'new address'
    dfm.loc[dfm.addr_y.isna(), 'status'] = 'deleted'
    dfm.loc[dfm.addr_x == dfm.addr_y, 'status'] = 'no change'
    dfm.loc[(dfm.addr_x.notna()) &
            (dfm.addr_y.notna()) &
            (dfm.addr_x != dfm.addr_y), 'status'] = 'address names changed'
    dfm.rename(columns={'addr_x': 'addr_prev', 'addr_y': 'addr_curr'},
               inplace=True)
    chlog = dfm.loc[dfm.status != 'no change'].reset_index(drop=True)
    return chlog


def compute_all_changes(pref1_dir: str, pref2_dir: str, save_dir: str) -> None:
    """Compute change logs for every prefixed file pair."""
    os.makedirs(save_dir, exist_ok=True)
    all_hex = '0123456789abcdef'
    for hex_prefix in all_hex:
        fn1 = os.path.join(pref1_dir, f'{hex_prefix}.fea')
        fn2 = os.path.join(pref2_dir, f'{hex_prefix}.fea')
        chlog = compare_two_prefixes(fn1, fn2)
        chlog_fn = os.path.join(save_dir, f'chlog_{hex_prefix}.fea')
        chlog.reset_index(drop=True).to_feather(chlog_fn)
        print(hex_prefix, end=' ', flush=True)
    print('Done.')


def merge_a_changelog(chlog_parts_dir: str, save_fn: str) -> None:
    """Merge all change log parts into a single deliverable file."""
    fl = os.listdir(chlog_parts_dir)
    fl = [x for x in fl if x.endswith('.fea')]
    full_fl = [os.path.join(chlog_parts_dir, x) for x in fl]
    data = [pd.read_feather(x) for x in full_fl]
    full_chlog = pd.concat(data, ignore_index=True)
    full_chlog.to_csv(save_fn, sep='¬', index=False)
    print(f'Saved to {save_fn}.')


def compute_changelog_fn(pcsv: str, dcsv: str) -> str:
    """Compute a file name for the change log file."""
    b1 = os.path.splitext(os.path.split(pcsv)[1])[0]
    b2 = os.path.splitext(os.path.split(dcsv)[1])[0]
    ddir = os.path.split(dcsv)[0]
    fn = os.path.join(ddir, f'{b1}_{b2}_change_log.csv')
    return fn


def delete_dir(dir_name: str) -> None:
    """Delete a directory and it's contents."""
    fl = os.listdir(dir_name)
    for fn in fl:
        os.remove(os.path.join(dir_name, fn))
    os.rmdir(dir_name)
    return None


def make_changelog(fias_fn1: str, fias_fn2: str, temp_dir: str) -> None:
    """Create a change log CSV file given two source FIAS CSV files."""
    os.makedirs(temp_dir, exist_ok=True)
    p1dir = os.path.join(temp_dir, 'parts_1')
    p2dir = os.path.join(temp_dir, 'parts_2')
    h1dir = os.path.join(temp_dir, 'hex_1')
    h2dir = os.path.join(temp_dir, 'hex_2')
    chdir = os.path.join(temp_dir, 'chlogparts')
    save_fn = compute_changelog_fn(fias_fn1, fias_fn2)
    disassemble_file(fias_fn1, p1dir)
    disassemble_file(fias_fn2, p2dir)
    assemble_by_prefix(p1dir, h1dir)
    assemble_by_prefix(p2dir, h2dir)
    delete_dir(p1dir)
    delete_dir(p2dir)
    compute_all_changes(h1dir, h2dir, chdir)
    delete_dir(h1dir)
    delete_dir(h2dir)
    merge_a_changelog(chdir, save_fn)
    delete_dir(chdir)
    return None
