"""
GAR
===

House list
----------

A sub-module that contains tools to make a formatted list of houses
from the source dataset.
"""

from collections import namedtuple

import pandas as pd


def h_wo_postcode(hs: pd.DataFrame, hp: pd.DataFrame) -> pd.DataFrame:
    """Select houses that do not have any postcode records."""
    oh = hs.loc[~hs.OBJECTID.isin(hp.OBJECTID)]
    return oh.reset_index(drop=True)


def h_no_current_postcode(hs: pd.DataFrame, hp: pd.DataFrame) -> pd.DataFrame:
    """Find houses which only have historic postcode records and no
    current postcode records.
    """
    hp_curr = hp.loc[hp.CHANGEIDEND == 0]
    h_nocurrp = hs.loc[~hs.OBJECTID.isin(hp_curr.OBJECTID)]
    return h_nocurrp.reset_index(drop=True)


def more_than_1_current(hp: pd.DataFrame) -> pd.DataFrame:
    """Find houses that have more than 1 current postcode records. This
    is an error and should be addressed.
    """
    hp_curr = hp.loc[hp.CHANGEIDEND == 0]
    g = hp_curr.groupby('OBJECTID').count().VALUE
    errors = g.loc[g > 1]
    return errors


def create_dummy_current(h_no_curr: pd.DataFrame) -> pd.DataFrame:
    """Takes in either:
       - a DataFrame of houses that only have historic records or
       - a DataFrame of houses that don't have house_params records at
         all
    and returns a DataFrame of dummy `house_params` records that
    represent current records for those houses.
    """
    dummy_ids = pd.Series(h_no_curr.OBJECTID.unique())
    hp_cols = ['OBJECTID', 'CHANGEIDEND', 'TYPEID', 'VALUE']
    dummy_current = pd.DataFrame(columns=hp_cols)
    dummy_current.OBJECTID = dummy_ids
    dummy_current.CHANGEIDEND = 0
    dummy_current.TYPEID = 5
    dummy_current.VALUE = ''
    return dummy_current.reset_index(drop=True)


def add_missing_hparams(hs: pd.DataFrame, hp: pd.DataFrame) -> pd.DataFrame:
    """Add missing house_params records for:
    1. orphaned houses (houses that don't have any house_params recs),
    2. houses which only have historic records and no current ones.
    """
    h_wo_pc = h_wo_postcode(hs, hp)
    h_wo_curr_pc = h_no_current_postcode(hs, hp)
    h_wo_pc_dc = create_dummy_current(h_wo_pc)
    h_wo_curr_pc_dc = create_dummy_current(h_wo_curr_pc)
    dupe_cols = ['OBJECTID', 'CHANGEIDEND', 'TYPEID']
    full_hp = hp.append(h_wo_pc_dc).append(h_wo_curr_pc_dc)
    full_hp = full_hp.drop_duplicates(dupe_cols)
    return full_hp.reset_index(drop=True)


def make_house_list(hs: pd.DataFrame, full_hp: pd.DataFrame) -> pd.DataFrame:
    """Compile a list of houses with historic PC change records."""
    # Remove history records for which there are no (active) houses
    hp_curr = full_hp.loc[full_hp.OBJECTID.isin(hs.OBJECTID)]
    hp_curr.reset_index(drop=True, inplace=True)
    # Add house info to every postcode history item
    hl = hp_curr.merge(hs, how='left', on='OBJECTID')
    return hl


def format_house_list(house_list: pd.DataFrame) -> pd.DataFrame:
    """Format houses with PC history to required GAR format."""
    hpc = house_list.copy()
    hpc['BUILDNUM'] = hpc['ADDNUM1']
    hpc['STRUCNUM'] = hpc['ADDNUM2']
    hpc['POSTALCODE'] = hpc['VALUE'].fillna('').astype(str)
    hpc['CURRENT'] = 0
    hpc.loc[hpc.CHANGEIDEND == 0, 'CURRENT'] = 1
    hr = hpc[['OBJECTGUID', 'CURRENT', 'POSTALCODE', 'HOUSENUM', 'BUILDNUM',
              'STRUCNUM', 'OBJECTID']].sort_values(by='OBJECTID')
    return hr


def full_house_list(hs: pd.DataFrame, hp: pd.DataFrame) -> pd.DataFrame:
    """Extract properly formatted house list from source data."""
    full_hp = add_missing_hparams(hs, hp)
    assert more_than_1_current(full_hp).empty
    hl = make_house_list(hs, full_hp)
    fhl = format_house_list(hl)
    return fhl
