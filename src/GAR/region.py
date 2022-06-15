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
from GAR.translit import translit_df


def add_region_iso_code(df: pd.DataFrame, reg_code: str) -> pd.DataFrame:
    """Add `region_iso_code` to the region dataframe."""
    rciso = {'01': 'AD', '02': 'BA', '03': 'BU', '04': 'AL', '05': 'DA',
             '06': 'IN', '07': 'KB', '08': 'KL', '09': 'KC', '10': 'KR',
             '11': 'KO', '12': 'ME', '13': 'MO', '14': 'SA', '15': 'SE',
             '16': 'TA', '17': 'TY', '18': 'UD', '19': 'KK', '20': 'CE',
             '21': 'CU', '22': 'ALT', '23': 'KDA', '24': 'KYA', '25': 'PRI',
             '26': 'STA', '27': 'KHA', '28': 'AMU', '29': 'ARK', '30': 'AST',
             '31': 'BEL', '32': 'BRY', '33': 'VLA', '34': 'VGG', '35': 'VLG',
             '36': 'VOR', '37': 'IVA', '38': 'IRK', '39': 'KGD', '40': 'KLU',
             '41': 'KAM', '42': 'KEM', '43': 'KIR', '44': 'KOS', '45': 'KGN',
             '46': 'KRS', '47': 'LEN', '48': 'LIP', '49': 'MAG', '50': 'MOS',
             '51': 'MUR', '52': 'NIZ', '53': 'NGR', '54': 'NVS', '55': 'OMS',
             '56': 'ORE', '57': 'ORL', '58': 'PNZ', '59': 'PER', '60': 'PSK',
             '61': 'ROS', '62': 'RYA', '63': 'SAM', '64': 'SAR', '65': 'SAK',
             '66': 'SVE', '67': 'SMO', '68': 'TAM', '69': 'TVE', '70': 'TOM',
             '71': 'TUL', '72': 'TYU', '73': 'ULY', '74': 'CHE', '75': 'ZAB',
             '76': 'YAR', '77': 'MOW', '78': 'SPE', '79': 'YEV', '83': 'NEN',
             '86': 'KHM', '87': 'CHU', '89': 'YAN', '91': 'CR', '92': 'SEV',
             '99': 'KZ-BAY'}
    res = df.copy()
    res['region_iso_code'] = rciso[reg_code]
    return res


def save_region(df: pd.DataFrame, save_fn: str) -> None:
    """Serialize ready region data to disk."""
    df.to_feather(save_fn)


def process_region(src_zip_fn: str, reg_code: str, dest_dir: str) -> None:
    """Extract region data from source archive, convert it as required
    by contract and return ready data as a pandas DataFrame.
    """
    t1 = time.perf_counter()
    data = load_all_data_unzipped(src_zip_fn, reg_code)
    logger.trace('load_all_data completed')
    data = cast_types(data)
    logger.trace('cast_types completed')
    data = filter_all(data)
    logger.trace('filter_all completed')
    f_hl = full_house_list(hs=data.hs, hp=data.hp)
    logger.trace('full_house_list completed')
    region = add_parents(hl=f_hl, mh=data.mh, ao=data.ao)
    logger.trace('add_parents completed')
    region = translit_df(region)
    logger.trace('translit_df completed')
    region = add_region_iso_code(region, reg_code)
    logger.trace('add_region_iso_code completed')
    region.columns = [x.lower() for x in region.columns]
    filename = os.path.join(dest_dir, f'{reg_code}.fea')
    save_region(region, filename)
    logger.trace('save_region completed')
    t2 = time.perf_counter()
    t = round(t2 - t1, 2)
    logger.success(f'Region {reg_code} done in {t} sec.')
