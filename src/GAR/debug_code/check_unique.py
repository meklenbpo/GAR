"""
GAR
===

Check unique
------------

A module that will check if certain combinations of "currency" fields
produce unique results for all regions.
"""

import datetime
import os
import sys
from typing import List

import pandas as pd

from GAR.cast_types import cast_types_ao
from GAR.cast_types import cast_types_munih


def load_region(fea_fn: str, asset_type: str) -> pd.DataFrame:
    """Load a source asset dataset from a feather file."""
    df = pd.read_feather(fea_fn)
    if asset_type == 'ao':
        cf = cast_types_munih(df)
    elif asset_type == 'mh':
        cf = cast_types_munih(df)
    else:
        raise ValueError('Unsupported `asset_type`')
    return cf


def c1(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #1 'ISACTIVE == 1' and return resulting
    DataFrame.
    """
    return df.loc[df.ISACTIVE == 1].reset_index(drop=True)


def c2(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #2 'ISACTUAL == 1' and return resulting
    DataFrame.
    """
    if asset_type == 'mh':
        return pd.DataFrame(columns=df.columns)
    return df.loc[df.ISACTUAL == 1].reset_index(drop=True)


def c3(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #3 'NEXTID == 0' and return resulting
    DataFrame.
    """
    return df.loc[df.NEXTID == 0].reset_index(drop=True)


def c4(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #4 'ENDDATE > today()' and return resulting
    DataFrame.
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return df.loc[df.ENDDATE > today].reset_index(drop=True)


def c5(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #5 'ISACTUAL == 1 and ISACTIVE == 1' and
    return resulting DataFrame.
    """
    if asset_type == 'mh':
        return pd.DataFrame(columns=df.columns)
    return df.loc[(df.ISACTUAL == 1) &
                  (df.ISACTIVE == 1)].reset_index(drop=True)


def c6(df: pd.DataFrame, asset_type: str) -> pd.DataFrame:
    """Apply combination #6 'ISACTUAL == 1 and ISACTIVE == 1 and
    NEXTID == 0 and ENDDATE > today()' and return resulting DataFrame.
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    if asset_type == 'ao':
        res = df.loc[(df.ISACTUAL == 1) & (df.ISACTIVE == 1) &
                     (df.NEXTID == 0) & (df.ENDDATE > today)]
    elif asset_type == 'mh':
        res = df.loc[(df.ISACTIVE == 1) & (df.NEXTID == 0) &
                     (df.ENDDATE > today)]
    else:
        raise ValueError('Unsupported `asset_type`.')
    return res.reset_index(drop=True)


def print_err(s: str) -> None:
    """Return a 'red' string."""
    print('\033[31m', end='', flush=True)
    print(s, end='', flush=True)
    print('\033[0m', end='', flush=True)


def print_ok(s: str) -> None:
    """Return a 'green' string."""
    print('\033[36m', end='', flush=True)
    print(s, end='', flush=True)
    print('\033[0m', end='', flush=True)


def isunique(df: pd.DataFrame) -> bool:
    """Check if OBJECTID records are unique in the `df` DataFrame."""
    if len(df.OBJECTID.unique()) == len(df):
        return True
    return False


def get_file_list(data_dir: str) -> List[str]:
    """Get a list of applicable file names from a directory."""
    full_file_list = sorted(os.listdir(data_dir))
    fea_file_list = [x for x in full_file_list if x.endswith('.fea')]
    pathname_list = [os.path.join(data_dir, x) for x in fea_file_list]
    return pathname_list


def code_from_fn(pathname: str) -> str:
    """Get OK2 code from dataset filename."""
    basename, _ = os.path.splitext(pathname)
    return basename[-2:]


def main() -> int:
    """script running function."""
    try:
        asset_type = sys.argv[1]
    except IndexError:
        print('Input `asset_type` as parameter.')
        return 1
    datadir = f'data/{asset_type}/'
    datalist = get_file_list(datadir)
    combinations = [c1, c2, c3, c4, c5, c6]
    print('')
    print(f'Computing {asset_type} combinations:')
    print('══════════════════════════')
    print('')
    print(f'{"Region ┃":>13}', end='', flush=True)
    print(f'{"C1 ┃":>13}', end='', flush=True)
    print(f'{"C2 ┃":>13}', end='', flush=True)
    print(f'{"C3 ┃":>13}', end='', flush=True)
    print(f'{"C4 ┃":>13}', end='', flush=True)
    print(f'{"C5 ┃":>13}', end='', flush=True)
    print(f'{"C6 ┃":>13}')
    for x in range(len(combinations)):
        print(f'{"━━━━━━━━━━━━╋":>13}', end='', flush=True)
    print(f'{"━━━━━━━━━━━━╋":>13}')
    for fn in datalist:
        ok2 = code_from_fn(fn)
        print(f'{ok2:>12}┃', end='', flush=True)
        data = load_region(fn, asset_type)
        for i, comb in enumerate(combinations):
            candidate = comb(data, asset_type)
            isuq = isunique(candidate)
            qty = str(len(candidate))
            cprint = print_ok if isuq else print_err
            cprint(f'{qty:>12}')
            print('┃', end='', flush=True)
        print()


if __name__ == '__main__':
    sys.exit(main())
