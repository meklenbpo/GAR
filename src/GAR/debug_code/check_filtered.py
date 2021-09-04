"""
GAR
===

Check filtered
--------------

A module that will check source data is ready to be processed into
FIAS, i.e. that filtered `mh` and `ao` datasets have only unique values
in OBJECTIDs columns.
"""

import os
import sys
from typing import List

import pandas as pd

from GAR.file_utils import get_list_of_regions
from GAR.cast_types import cast_types_ao
from GAR.cast_types import cast_types_mh
from GAR.filter_data import filter_ao, filter_mh


def load_region(fea_fn: str, asset_type: str) -> pd.DataFrame:
    """Load a source asset dataset from a feather file."""
    df = pd.read_feather(fea_fn)
    if asset_type == 'ao':
        cf = cast_types_ao(df)
    elif asset_type == 'mh':
        cf = cast_types_mh(df)
    else:
        raise ValueError('Unsupported `asset_type`')
    return cf


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


def print_header() -> None:
    """Print table header."""
    print('')
    print('Checking AO and MH:')
    print('══════════════════════════')
    print('')
    print(f'{"Region ┃":>13}', end='', flush=True)
    print(f'{"AO ┃":>13}', end='', flush=True)
    print(f'{"MH ┃":>13}')
    for _ in range(2):
        print(f'{"━━━━━━━━━━━━╋":>13}', end='', flush=True)


def main() -> int:
    """script running function."""
    print_header()
    zfn = 'data/gar_xml.zip'
    aodatadir = 'data/ao/'
    mhdatadir = 'data/mh/'
    regions = get_list_of_regions(zfn)
    print(f'{"━━━━━━━━━━━━╋":>13}')
    for ok2 in regions:
        print(f'{ok2:>12}┃', end='', flush=True)
        aofn = os.path.join(aodatadir, f'ao{ok2}.fea')
        mhfn = os.path.join(mhdatadir, f'mh{ok2}.fea')
        try:
            aodata = load_region(aofn, 'ao')
        except FileNotFoundError:
            qty = 'n/a'
            print(f'{qty:>12}')
            print('┃', end='', flush=True)
            continue
        try:
            mhdata = load_region(mhfn, 'mh')
        except FileNotFoundError:
            qty = 'n/a'
            print(f'{qty:>12}')
            print('┃', end='', flush=True)
            print('')
            continue
        try:
            fao = filter_ao(aodata)
        except AssertionError:
            cprint = print_err
        else:
            cprint = print_ok
        aoqty = str(len(fao))
        cprint(f'{aoqty:>12}')
        print('┃', end='', flush=True)
        try:
            fmh = filter_mh(mhdata, fao)
        except AssertionError:
            cprint = print_err
        else:
            cprint = print_ok
        mhqty = str(len(fmh))
        cprint(f'{mhqty:>12}')
        print('┃', end='', flush=True)
        print()


if __name__ == '__main__':
    sys.exit(main())


for x, ok2 in zip(fns, regions):
    d = load_all_data('data/gar_xml.zip', ok2)
    lhs, lhp, lao, lmh = len(d.hs), len(d.hp), len(d.ao), len(d.mh)
    print(f'{ok2:>12}|{lhs:>12}|{lhp:>12}|{lao:>12}|{lmh:>12}')
