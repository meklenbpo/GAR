"""
GAR
===

Main
----

A module that acts as the coordinating processor of the GAR dataset.
"""

import os
import sys

from loguru import logger

from GAR.file_utils import get_list_of_active_regions
from GAR.region import process_region
from GAR.merge_to_csv import merge_all_files
from GAR.changelog import make_changelog


def process_all_regions(src_zip_fn: str, dest_dir: str) -> None:
    """Process all regions one by one in a multiprocess pool and
    serialize the results to a `dest_dir` directory.
    """
    os.makedirs(dest_dir, exist_ok=True)
    regions = get_list_of_active_regions(src_zip_fn)
    for region in regions:
        process_region(src_zip_fn, region, dest_dir)
    print('')


def greeting() -> None:
    """Print a greeting."""
    print('')
    print('Computing GAR:')
    print('══════════════')
    print('')


def syntax_error() -> None:
    """Print a syntax error message."""
    print('Syntax Error.')
    print(f'Usage: {__file__} [source_gar_xml.zip] [tmpdir] [ready_fias.csv] '
          '[previous_fias.csv]')
    print('')


def remove_temp_dir(ddir: str) -> None:
    """Delete temporary files and the temp dir itself."""
    fl = os.listdir(ddir)
    for filename in fl:
        os.remove(os.path.join(ddir, filename))
    os.rmdir(ddir)


def main() -> int:
    """Script running function."""
    greeting()
    if len(sys.argv) != 5:
        syntax_error()
        return 1
    zfn = sys.argv[1]
    ddir = sys.argv[2]
    dcsv = sys.argv[3]
    pcsv = sys.argv[4]
    logger.info(f'Starting processing {zfn} -> {ddir}')
    process_all_regions(zfn, ddir)
    logger.info(f'Processing complete. Starting export {ddir} -> {dcsv}')
    merge_all_files(ddir, dcsv)
    logger.success('Export complete.')
    remove_temp_dir(ddir)
    logger.info(f'Export complete. Starting change log {pcsv}:{dcsv}')
    make_changelog(pcsv, dcsv, ddir)
    logger.success('All done.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
