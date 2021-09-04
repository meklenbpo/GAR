"""
GAR
---

A prototype script to extract useful information from GAR.
"""

import os
from typing import List

from lxml import etree
import pandas as pd
from sqlalchemy import create_engine


psql = create_engine('postgresql://yk@localhost:5432/gar')


def load_full_xml(fn: str, tag: str) -> pd.DataFrame:
    """Load all content from an XML with all attributes."""
    xml = etree.iterparse(fn, tag=tag, events=['end'])
    rows = []
    while True:
        try:
            _, element = next(xml)
        except StopIteration:
            break
        row = dict(element.attrib)
        rows.append(row)
    df = pd.DataFrame(rows)
    return df


def load_subset_xml(fn: str, tag: str, req_attr: List[str]) -> pd.DataFrame:
    """Load only required attributes from an XML file into a
    DataFrame.
    """
    xml = etree.iterparse(fn, tag=tag, events=['end'])
    rows = []
    for _, element in xml:
        row = {}
        for attr in req_attr:
            row[attr] = element.get(attr)
        rows.append(row)
    df = pd.DataFrame(rows)
    return df


def load_xml_head(fn: str, tag: str) -> pd.DataFrame:
    """Load first 100 tags from an XML with all attributes."""
    xml = etree.iterparse(fn, tag=tag, events=['end'])
    rows = []
    counter = 0
    while True:
        try:
            counter += 1
            if counter > 100:
                break
            _, element = next(xml)
        except StopIteration:
            break
        row = dict(element.attrib)
        rows.append(row)
    df = pd.DataFrame(rows)
    return df


fl = {
    'ADDR_OBJ.XML': 'OBJECT',
    'ADDR_OBJ_DIVISION.XML': 'ITEM',
    'ADDR_OBJ_PARAMS.XML': 'PARAM',
    'ADM_HIERARCHY.XML': 'ITEM',
    'APARTMENTS.XML': 'APARTMENT',
    'APARTMENTS_PARAMS.XML': 'PARAM',
    'CARPLACES.XML': 'CARPLACE',
    'CARPLACES_PARAMS.XML': 'PARAM',
    'CHANGE_HISTORY.XML': 'ITEM',
    'HOUSES.XML': 'HOUSE',
    'HOUSES_PARAMS.XML': 'PARAM',
    'MUN_HIERARCHY.XML': 'ITEM',
    'NORMATIVE_DOCS.XML': 'NORMDOC',
    'REESTR_OBJECTS.XML': 'OBJECT',
    'ROOMS.XML': 'ROOM',
    'ROOMS_PARAMS.XML': 'PARAM',
    'STEADS.XML': 'STEAD',
    'STEADS_PARAMS.XML': 'PARAM',
    'R_ADDHOUSE_TYPES.XML': 'HOUSETYPE',
    'R_ADDR_OBJ_TYPES.XML': 'ADDRESSOBJECTTYPE',
    'R_APARTMENT_TYPES.XML': 'APARTMENTTYPE',
    'R_HOUSE_TYPES.XML': 'HOUSETYPE',
    'R_NORMATIVE_DOCS_KINDS.XML': 'NDOCKIND',
    'R_NORMATIVE_DOCS_TYPES.XML': 'NDOCTYPE',
    'R_OBJECT_LEVELS.XML': 'OBJECTLEVEL',
    'R_OPERATION_TYPES.XML': 'OPERATIONTYPE',
    'R_PARAM_TYPES.XML': 'PARAMTYPE',
    'R_ROOM_TYPES.XML': 'ROOMTYPE'
    }


def all_to_sql(samples_dir: str, samples_meta: dict) -> None:
    """Load sample data to a DB."""
    psql = create_engine('postgresql://yk@localhost:5432/gar')
    for filename in samples_meta:
        full_fn = os.path.join(samples_dir, filename)
        tag = samples_meta[filename]
        table_name, _ = os.path.splitext(filename)
        df = load_full_xml(os.path.join(samples_dir, filename), tag)
        cols = list(df.columns)
        lcols = [x.lower() for x in cols]
        df.columns = lcols
        df.to_sql(table_name.lower(), psql, method='multi')
        print(table_name)
    psql.dispose()
