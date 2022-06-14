"""
GAR
===

Merge files to CSV
------------------

Merge all region files into a single CSV file.
"""

import os

from loguru import logger
import pandas as pd


def merge_all_files(src_dir: str, dest_fn: str) -> None:
    """Merge all individual region files into a single CSV file."""
    official_columns = ['id', 'guid', 'current', 'postalcode', 'housenum',
                        'buildnum', 'strucnum', 'housenum_en', 'buildnum_en',
                        'strucnum_en', 'street_s', 'street_f', 'street_s_en',
                        'street_f_en', 'terr_s', 'terr_f', 'terr_s_en',
                        'terr_f_en', 'place_s', 'place_f', 'place_s_en',
                        'place_f_en', 'city_s', 'city_f', 'city_s_en',
                        'city_f_en', 'muni_s', 'muni_f', 'muni_s_en',
                        'muni_f_en', 'munr_s', 'munr_f', 'munr_s_en',
                        'munr_f_en', 'region_s', 'region_f', 'region_s_en',
                        'region_f_en', 'region_iso_code']
    fl = os.listdir(src_dir)
    fl = [x for x in fl if x[-4:] == '.fea']
    fl = sorted([os.path.join(src_dir, x) for x in fl])
    for fn in fl:
        logger.success(f'Merging {fn}...')
        df = pd.read_feather(fn).reset_index()
        df.rename(columns={'index': 'id', 'objectguid': 'guid'}, inplace=True)
        if not os.path.isfile(dest_fn):
            df[official_columns].to_csv(dest_fn, sep='\u00ac', index=False)
        else:
            df[official_columns].to_csv(dest_fn, mode='a', sep='\u00ac',
                                        header=False, index=False)
    return None
