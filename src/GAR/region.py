"""
GAR
===

Region
------

A submodule of the GAR package that contains functions necessary to
process a single region of GAR address data.
"""

import os
import time

from loguru import logger
import pandas as pd

from GAR.cast_types import cast_types
from GAR.file_utils import load_all_data
from GAR.filter_data import filter_all
from GAR.house_list import full_house_list
from GAR.parents import add_parents


def save_region(df: pd.DataFrame, save_fn: str) -> None:
    """Serialize ready region data to disk."""
    df.to_feather(save_fn)


def process_region(src_zip_fn: str, reg_code: str,
                   dest_dir: str) -> pd.DataFrame:
    """Extract region data from source archive, convert it as required
    by contract and return ready data as a pandas DataFrame.
    """
    print(f'{reg_code:>12}┃', end='', flush=True)
    t0 = time.time()
    data = load_all_data(src_zip_fn, reg_code)
    t1 = time.time()
    d1 = round(t1 - t0, 1)
    print(f'{d1:>12}┃', end='', flush=True)
    data = cast_types(data)
    data = filter_all(data)
    f_hl = full_house_list(hs=data.hs, hp=data.hp)
    t2 = time.time()
    d2 = round(t2 - t1, 1)
    print(f'{d2:>12}┃', end='', flush=True)
    region = add_parents(hl=f_hl, mh=data.mh, ao=data.ao)
    t3 = time.time()
    d3 = round(t3 - t2, 1)
    print(f'{d3:>12}┃', end='', flush=True)
    filename = os.path.join(dest_dir, f'{reg_code}.fea')
    save_region(region, filename)
    t4 = time.time()
    d4 = round(t4 - t3, 1)
    d5 = round(t4 - t0, 1)
    print(f'{d4:>12}┃', end='', flush=True)
    print(f'{d5:>12}┃')
    return region.reset_index(drop=True)
