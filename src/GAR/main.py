"""
GAR
===

Main
----

A module that acts as the coordinating processor of the GAR dataset.
"""

from GAR.file_utils import get_list_of_regions
from GAR.region import process_region
from GAR.region import save_region


def process_all_regions(src_zip_fn: str, dest_dir: str) -> None:
    """Process all regions one by one and serialize the results to a
    `dest_dir` directory.
    """
    print('')
    print('Computing GAR:')
    print('══════════════')
    print('')
    print(f'{"Region ┃":>13}', end='', flush=True)
    print(f'{"zip/XML ┃":>13}', end='', flush=True)
    print(f'{"Filtering ┃":>13}', end='', flush=True)
    print(f'{"Parents ┃":>13}', end='', flush=True)
    print(f'{"Saving ┃":>13}', end='', flush=True)
    print(f'{"Total ┃":>13}')
    for x in range(6):
        print(f'{"━━━━━━━━━━━━╋":>13}', end='', flush=True)
    print('')
    regions = get_list_of_regions(src_zip_fn)
    for region in regions:
        df = process_region(src_zip_fn, region, dest_dir)
    print('')


process_all_regions('data/gar_xml.zip', 'data/')