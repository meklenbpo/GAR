"""
GAR
===

File utils
----------

A submodule of the GAR package that takes care of file I/O operations.
Includes:
1.  Unzip region from a source distribution
2.  Read XML file into a pandas DataFrame
3.  Concatenate ready region files
"""

from collections import namedtuple
from typing import List
import zipfile

from loguru import logger
from lxml import etree
import pandas as pd


def test_src_availability(fn: str) -> bool:
    """Test if the source GAR zip file is available in it's location
    and is not corrupt.
    """
    logger.info(f'Testing {fn}...')
    try:
        z = zipfile.ZipFile(fn)
    except FileNotFoundError:
        logger.error(f'File {fn} not found.')
        return False
    status = z.testzip()
    z.close()
    if status:
        logger.error(f'File {fn} is corrupt.')
        return False
    logger.success(f'File {fn} is available and not corrupt.')
    return True


def get_list_of_regions(zipfn: str) -> List[str]:
    """Read the contents of the zip file and return a list of regions
    that it contains.
    """
    with zipfile.ZipFile(zipfn) as z:
        fl = z.namelist()
    regfl = set([x[:2] for x in fl])
    regfl.remove('AS')
    return sorted(list(regfl))


def get_region_filenames(zipfn: str, ok2: str) -> namedtuple:
    """Read the contents of the GAR zipfile and return the names of the
    four files required for region processing.
    """
    Files = namedtuple('Files', 'hs hp ao mh')
    with zipfile.ZipFile(zipfn) as z:
        fl = z.namelist()
    regfl = [x for x in fl if x.startswith(f'{ok2}/')]
    hsfn = sorted([x for x in regfl if x.startswith(f'{ok2}/AS_HOUSES')])
    aofn = [x for x in regfl if x.startswith(f'{ok2}/AS_ADDR_OBJ')]
    aofn = [x for x in aofn if not ('PARAMS' in x or 'DIVISION' in x)]
    mhfn = [x for x in regfl if x.startswith(f'{ok2}/AS_MUN_HIERARCHY')]
    return Files(hs=hsfn[0], hp=hsfn[1], ao=aofn[0], mh=mhfn[0])


def load_xml_from_zip(zfn: str, xmlfn: str, tag: str,
                      req_attr: List[str]) -> pd.DataFrame:
    """Access an XML file (`xmlfn`) stored inside a zip file (`zfn`).
    Iteratively read through it and load only required attributes (`req_attr`)
    into a pandas DataFrame.
    """
    z = zipfile.ZipFile(zfn)
    xml = etree.iterparse(z.open(xmlfn), tag=tag, events=['end'])
    rows = []
    for _, element in xml:
        row = {}
        for attr in req_attr:
            row[attr] = element.get(attr)
        rows.append(row)
    df = pd.DataFrame(rows)
    z.close()
    return df


def load_all_data(zfn: str, region: str) -> namedtuple:
    """Load all required files from a zip archive into a namedtuple of
    DataFrames.
    """
    Data = namedtuple('Data', 'hs hp mh ao')
    hs_attrs = ['OBJECTID', 'OBJECTGUID', 'HOUSENUM', 'HOUSETYPE', 'ISACTIVE',
                'ADDNUM1', 'ADDTYPE1', 'ADDNUM2', 'ADDTYPE2']
    hp_attrs = ['OBJECTID', 'CHANGEIDEND', 'TYPEID', 'VALUE']
    ao_attrs = ['OBJECTID', 'NAME', 'TYPENAME', 'LEVEL', 'ISACTIVE',
                'ISACTUAL', 'NEXTID', 'ENDDATE']
    mh_attrs = ['OBJECTID', 'PARENTOBJID', 'OKTMO', 'ISACTIVE', 'NEXTID',
                'ENDDATE']
    filenames = get_region_filenames(zfn, region)
    data = Data(
        hs=load_xml_from_zip(zfn, filenames.hs, 'HOUSE', hs_attrs),
        hp=load_xml_from_zip(zfn, filenames.hp, 'PARAM', hp_attrs),
        mh=load_xml_from_zip(zfn, filenames.mh, 'ITEM', mh_attrs),
        ao=load_xml_from_zip(zfn, filenames.ao, 'OBJECT', ao_attrs)
    )
    return data
