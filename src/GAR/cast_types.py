"""
GAR
===

Cast types
----------

A submodule of the GAR package that contains functions that convert
source data fields to proper types.
"""

from collections import namedtuple

import pandas as pd


def cast_types_hs(data: pd.DataFrame) -> pd.DataFrame:
    """Cast houses DataFrame columns to the correct types."""
    df = data.copy()
    assert df.loc[df.OBJECTID.isna()].empty
    assert df.loc[df.OBJECTGUID.isna()].empty
    assert df.OBJECTGUID.str.len().min() == df.OBJECTGUID.str.len().max() == 36
    df.OBJECTID = df.OBJECTID.astype(int)
    df.OBJECTGUID = df.OBJECTGUID.astype(str)
    df.HOUSENUM = df.HOUSENUM.fillna('').astype(str)
    df.ADDNUM1 = df.ADDNUM1.fillna('').astype(str)
    df.ADDNUM2 = df.ADDNUM2.fillna('').astype(str)
    df.HOUSETYPE = df.HOUSETYPE.fillna(0).astype(int)
    df.ADDTYPE1 = df.ADDTYPE1.fillna(0).astype(int)
    df.ADDTYPE2 = df.ADDTYPE1.fillna(0).astype(int)
    df.ISACTIVE = df.ISACTIVE.fillna(0).astype(int)
    return df


def cast_types_hp(data: pd.DataFrame) -> pd.DataFrame:
    """Cast house_params DataFrame columns to the right types."""
    df = data.copy()
    assert df.loc[df.OBJECTID.isna()].empty
    df.OBJECTID = df.OBJECTID.astype(int)
    df.CHANGEIDEND = df.CHANGEIDEND.astype(int)
    df.TYPEID = df.TYPEID.astype(int)
    df.VALUE = df.VALUE.fillna('').astype(str)
    return df


def cast_types_ao(data: pd.DataFrame) -> pd.DataFrame:
    """Cast muni_hierarchy DataFrame columns to the right types."""
    df = data.copy()
    assert df.loc[df.OBJECTID.isna()].empty
    df.OBJECTID = df.OBJECTID.astype(int)
    df.NAME = df.NAME.fillna('').astype(str)
    df.TYPENAME = df.TYPENAME.fillna('').astype(str)
    df.LEVEL = df.LEVEL.astype(int)
    df.ISACTIVE = df.ISACTIVE.astype(int)
    # next 3 are for debug
    df.ISACTUAL = df.ISACTUAL.astype(int)
    df.NEXTID = df.NEXTID.fillna(0).astype(int)
    df.ENDDATE = df.ENDDATE.fillna('').astype(str)
    return df


def cast_types_mh(data: pd.DataFrame) -> pd.DataFrame:
    """Cast addr_obj DataFrame columns to the right types."""
    df = data.copy()
    assert df.loc[df.OBJECTID.isna()].empty
    df.OBJECTID = df.OBJECTID.astype(int)
    df.PARENTOBJID = df.PARENTOBJID.fillna(0).astype(int)
    df.OKTMO = df.OKTMO.fillna('').astype(str)
    df.ISACTIVE = df.ISACTIVE.astype(int)
    df.NEXTID = df.NEXTID.fillna(0).astype(int)
    df.ENDDATE = df.ENDDATE.fillna('').astype(str)
    return df


def cast_types(data: namedtuple) -> namedtuple:
    """Cast columns to correct types in all four source DataFrames."""
    Data = namedtuple('Data', 'hs hp mh ao')
    cast = Data(
        hs=cast_types_hs(data.hs),
        hp=cast_types_hp(data.hp),
        mh=cast_types_mh(data.mh),
        ao=cast_types_ao(data.ao)
    )
    return cast
