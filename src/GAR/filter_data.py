"""
GAR
===

Filter data
-----------

A submodule that contains tools to filter the source data and keep only
the records that are required for GAR processing.
"""

from collections import namedtuple

import pandas as pd


def filter_hs(src_hs: pd.DataFrame) -> pd.DataFrame:
    """Take source houses dataframe (as imported from XML and cast into
    proper types) and filter redundand records out:
    1. Keep only active records
    2. Keep only records of certain types (discard garages, mines, etc.)
    """
    hf = src_hs.loc[src_hs.ISACTIVE == 1]
    hf = hf.loc[hf.HOUSETYPE.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
    hf = hf.loc[hf.ADDTYPE1.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
    hf = hf.loc[hf.ADDTYPE2.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
    return hf.reset_index(drop=True)


def filter_hp(src_hp: pd.DataFrame, f_hs: pd.DataFrame) -> pd.DataFrame:
    """Process `src_hp` (parameter change history) dataset to remove
    the records that are not required in GAR processing:
    1. Keep only postcode changes
    2. Remove duplicates
    3. Keep house_params records only for filtered houses (see.
       filter_houses function).
    """
    hp = src_hp.copy()
    hpf = hp.loc[hp.TYPEID == 5]
    hpf = hpf.sort_values(by=['OBJECTID', 'CHANGEIDEND'])
    hpf = hpf.drop_duplicates(['OBJECTID', 'VALUE'])
    hpf = hpf.loc[hpf.OBJECTID.isin(f_hs.OBJECTID)]
    return hpf.reset_index(drop=True)


def filter_ao(src_ao: pd.DataFrame) -> pd.DataFrame:
    """Process `ao` dataset to keep only records that are reuqired in
    GAR processing - i.e. only active ao.
    """
    aof = src_ao.loc[src_ao.ISACTIVE == 1].copy()
    assert len(aof.OBJECTID.unique()) == len(aof)
    aof.drop('ISACTIVE', axis=1, inplace=True)
    return aof.reset_index(drop=True)


def filter_mh(src_mh: pd.DataFrame, f_hs: pd.DataFrame,
              f_ao: pd.DataFrame) -> pd.DataFrame:
    """Process `muni_hierarchy` dataset to keep only records that are
    required for GAR processing:
    1. Keep only `active` muni_hierarchy records,
    2. Keep records only for `active` houses and `active` AO,
    3. Keep only units, whose parents are active AO.
    """
    mhf = src_mh.loc[src_mh.ISACTIVE == 1].copy()
    mhf = mhf.loc[mhf.OBJECTID.isin(f_hs.OBJECTID) |
                  mhf.OBJECTID.isin(f_ao.OBJECTID)].copy()
    mhf = mhf.loc[mhf.PARENTOBJID.isin(f_ao.OBJECTID)].copy()
    # Add level info to filter out duplicates (highest level prefered)
    mhm = mhf.merge(f_ao[['OBJECTID', 'LEVEL']], how='left',
                    left_on='PARENTOBJID', right_on='OBJECTID',
                    suffixes=('', 'x'))
    mhm.sort_values(['OBJECTID', 'PARENTOBJID', 'LEVEL'], inplace=True)
    mhm.drop_duplicates(['OBJECTID'], inplace=True, ignore_index=True)
    mhf = mhm[mhf.columns].copy()
    assert len(mhf.OBJECTID.unique()) == len(mhf)
    mhf.drop('ISACTIVE', axis=1, inplace=True)
    return mhf.reset_index(drop=True)


def filter_all(data: namedtuple) -> namedtuple:
    """Filter all four datasets required for GAR processing and return
    a namedtuple that contains filtered results.
    """
    Data = namedtuple('Data', 'hs hp mh ao')
    fhs = filter_hs(data.hs)
    fhp = filter_hp(data.hp, fhs)
    fao = filter_ao(data.ao)
    fmh = filter_mh(data.mh, fhs, fao)
    filtered = Data(hs=fhs, hp=fhp, ao=fao, mh=fmh)
    return filtered
