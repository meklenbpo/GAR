"""
GAR
===

Parents
-------

A submodule of the GAR package that contains functions to traverse the
addresses parents hierarchy.
"""

import math

import pandas as pd


def fill_parents(hl: pd.DataFrame, mh: pd.DataFrame,
                 ao: pd.DataFrame) -> pd.DataFrame:
    """Fill parent information for every house subsequently."""
    p = hl.copy()
    p.rename(columns={'OBJECTID': 'px'}, inplace=True)
    mhf = mh[['OBJECTID', 'PARENTOBJID']].copy()
    aof = ao[['OBJECTID', 'NAME', 'TYPENAME', 'LEVEL']].copy()
    for i in range(1, 7):
        p = p.merge(mhf, how='left', left_on='px', right_on='OBJECTID')
        p = p.drop('OBJECTID', axis=1)
        p = p.merge(aof, how='left', left_on='PARENTOBJID', right_on='OBJECTID')
        p = p.rename(columns={'px':f'p{i - 1}'})
        p = p.rename(columns={'NAME':f'n{i}', 'TYPENAME':f't{i}', 'LEVEL':f'l{i}'})
        p = p.drop('PARENTOBJID', axis=1)
        p = p.rename(columns={'OBJECTID':'px'})
    maxlevel = p.l6.max()
    assert math.isnan(maxlevel) or maxlevel == 1  # i.e. top level is `region`
    return p.reset_index(drop=True)


def format_final(hl_w_parents: pd.DataFrame) -> pd.DataFrame:
    """Prepare a formatted stub of the final table, i.e. create and
    fill all required columns with default values.
    """
    hcols = ['OBJECTGUID', 'CURRENT', 'POSTALCODE', 'HOUSENUM', 'BUILDNUM',
             'STRUCNUM']
    fl = hl_w_parents[hcols].copy()
    levels = ['STREET', 'TERR', 'PLACE', 'CITY', 'MUNI', 'MUNR', 'ADMR',
              'REGION']
    for level in levels:
        fl[f'{level}_S'] = ''
        fl[f'{level}_F'] = ''
    return fl


def spread_out_parents_by_level(format_hl: pd.DataFrame, parents_hl: pd.DataFrame) -> pd.DataFrame:
    """Put values into their proper columns by level."""
    r = format_hl.copy()
    p = parents_hl.copy()
    levels = ['STREET', 'TERR', 'PLACE', 'CITY', 'MUNI', 'MUNR', 'ADMR',
              'REGION']
    for idx, level in enumerate(levels[::-1], 1):
        for col in range(1, 7):
            r.loc[p[f'l{col}'] == idx, f'{level}_S'] = p[f't{col}']
            r.loc[p[f'l{col}'] == idx, f'{level}_F'] = p[f'n{col}']
    return r.reset_index(drop=True)


def add_parents(hl: pd.DataFrame, mh: pd.DataFrame,
                ao: pd.DataFrame) -> pd.DataFrame:
    """Take formatted house list, muni_hierarchy source dataset and ao
    source dataset and compile all three into the ready region table.
    """
    parents = fill_parents(hl=hl, mh=mh, ao=ao)
    empty_t = format_final(parents)
    final = spread_out_parents_by_level(empty_t, parents)
    final.drop('ADMR_S', axis=1, inplace=True)
    final.drop('ADMR_F', axis=1, inplace=True)
    return final.reset_index(drop=True)
