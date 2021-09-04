"""
GAR
===

Unzip assets
------------

A module that unzips source XML files of a given type (asset) for a
list of regions and saves them to local directory.
"""

import os
import sys
from typing import List

from GAR.file_utils import get_list_of_regions
from GAR.file_utils import get_region_filenames
from GAR.file_utils import load_xml_from_zip


# Config constants
HSATTR = ['OBJECTID', 'OBJECTGUID', 'HOUSENUM', 'HOUSETYPE', 'ISACTIVE',
          'ADDNUM1', 'ADDTYPE1', 'ADDNUM2', 'ADDTYPE2']
HPATTR = ['OBJECTID', 'CHANGEIDEND', 'TYPEID', 'VALUE']
AOATTR = ['OBJECTID', 'NAME', 'TYPENAME', 'LEVEL', 'ISACTIVE', 'ISACTUAL',
          'NEXTID', 'ENDDATE']
MHATTR = ['OBJECTID', 'PARENTOBJID', 'OKTMO', 'ISACTIVE', 'NEXTID', 'ENDDATE']

TAGS = {'hs': 'HOUSE', 'hp': 'PARAM', 'ao': 'OBJECT', 'mh': 'ITEM'}


def extract_all(zfn: str, region_list: List[str], asset_type: str,
                dest_dir: str) -> None:
    """Extract all source XML files for a given list of regions for a
    given `asset_type` and save them to `dest_dir` as feather
    datasets.
    `asset_type` can be `hs`, `hp`, `ao` or `mh`.
    """
    assert asset_type in ['hs', 'hp', 'ao', 'mh']
    rfn_tuples = [get_region_filenames(zfn, ok2) for ok2 in region_list]
    rfns = [x.__getattribute__(asset_type) for x in rfn_tuples]
    tag = TAGS[asset_type]
    attrs = (HSATTR if asset_type == 'hs' else
             HPATTR if asset_type == 'hp' else
             AOATTR if asset_type == 'ao' else
             MHATTR)
    os.makedirs(dest_dir, exist_ok=True)
    for rfn, ok2 in zip(rfns, region_list):
        data = load_xml_from_zip(zfn, rfn, tag, attrs)
        if data.empty:
            print(f'OK2:{ok2} - no data')
            continue
        datafn = os.path.join(dest_dir, f'{asset_type}{ok2}.fea')
        data.to_feather(datafn)
        print(datafn)


def main() -> int:
    """Script running function."""
    try:
        asset_type = sys.argv[1]
    except IndexError:
        print('Input `asset_type` as parameter.')
        return 1
    assert asset_type in ['hs', 'hp', 'ao', 'mh']
    zfn = 'data/gar_xml.zip'
    dest_dir = f'data/{asset_type}/'
    region_list = get_list_of_regions(zfn)
    print(f'Extracting {asset_type} to {dest_dir}')
    print('======================================')
    extract_all(zfn, region_list, asset_type, dest_dir)
    return 0


if __name__ == '__main__':
    sys.exit(main())
