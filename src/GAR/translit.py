"""
GAR
===

Translit
--------

A submodule of the GAR package that contains functions necessary to
transliterate data.
"""

import pandas as pd


def translit_series(cyr: pd.Series) -> pd.Series:
    """Take a string in cyrillic script and return its equivalent in
    Latin.
    """
    lower = {'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
             'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й':'y', 'к': 'k',
             'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
             'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
             'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ь': 'ь', 'ы': 'y', 'ъ': 'ъ',
             'э': 'e', 'ю': 'yu', 'я': 'ya'}
    lowertr = str.maketrans(lower)
    lat = cyr.str.translate(lowertr)
    upper = {'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'Ye',
             'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й':'Y', 'К': 'K',
             'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R',
             'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts',
             'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch', 'Ы': 'Y', 'Э': 'E', 'Ю': 'Yu',
             'Я': 'Ya'}
    uppertr = str.maketrans(upper)
    lat = lat.str.translate(uppertr)
    vowelsr = ' AaEeOoIiUuYyьъ'
    lat = lat.str.replace(fr'([{vowelsr}])e', r'\g<1>ye', regex=True)
    lat = lat.str.replace(r'[A-Z][a-z][A-Z]', lambda x: x.group(0).upper(),
                          regex=True)
    lat = lat.str.replace('ь', '')
    lat = lat.str.replace('ъ', '\'')
    return lat


def translit_df(src: pd.DataFrame) -> pd.DataFrame:
    """Create latin equivalent copies of all cyrillic-containing
    columns in the dataframe."""
    df = src.copy()
    df['HOUSENUM_EN'] = translit_series(df['HOUSENUM'])
    df['BUILDNUM_EN'] = translit_series(df['BUILDNUM'])
    df['STRUCNUM_EN'] = translit_series(df['STRUCNUM'])
    df['STREET_S_EN'] = translit_series(df['STREET_S'])
    df['STREET_F_EN'] = translit_series(df['STREET_F'])
    df['TERR_S_EN'] = translit_series(df['TERR_S'])
    df['TERR_F_EN'] = translit_series(df['TERR_F'])
    df['PLACE_S_EN'] = translit_series(df['PLACE_S'])
    df['PLACE_F_EN'] = translit_series(df['PLACE_F'])
    df['CITY_S_EN'] = translit_series(df['CITY_S'])
    df['CITY_F_EN'] = translit_series(df['CITY_F'])
    df['MUNI_S_EN'] = translit_series(df['MUNI_S'])
    df['MUNI_F_EN'] = translit_series(df['MUNI_F'])
    df['MUNR_S_EN'] = translit_series(df['MUNR_S'])
    df['MUNR_F_EN'] = translit_series(df['MUNR_F'])
    df['REGION_S_EN'] = translit_series(df['REGION_S'])
    df['REGION_F_EN'] = translit_series(df['REGION_F'])
    return df
