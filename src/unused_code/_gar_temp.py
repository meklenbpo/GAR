"""
GAR
---

A script that processes standard GAR data:
- houses
- houses_params
- mun_hierarchy
- addr_obj

and transforms it into a format required by the customer:
1.  All addresses listed in a flat table
2.  Every address has it's parent object specified by levels
3.  Includes historic postcode vales for every house
4.  Includes current address objects even if they don't have children
"""

from loguru import logger

from gar_to_sql import load_subset_xml



# Filtering the data
# houses
hf = h.loc[h.ISACTIVE == 1]
hf = hf.loc[hf.HOUSETYPE.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
hf = hf.loc[hf.ADDTYPE1.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
hf = hf.loc[hf.ADDTYPE2.isin([0, 1, 2, 3, 5, 7, 8, 9, 10])]
# houses params
hpf = hp.loc[hp.TYPEID == 5]
hpf = hpf.sort_values(by=['OBJECTID', 'CHANGEIDEND'])
hpf = hpf.drop_duplicates(['OBJECTID', 'VALUE'])
hpf = hpf.loc[hpf.OBJECTID.isin(hf.OBJECTID)]
# muni_hierarchy
mhf = mh.loc[mh.ISACTIVE == 1].copy()
assert len(mhf.OBJECTID.unique()) == len(mhf)
mhf.drop('ISACTIVE', axis=1, inplace=True)
# ao
aof = ao.loc[ao.ISACTIVE == 1].copy()
assert len(aof.OBJECTID.unique()) == len(aof)
aof.drop('ISACTIVE', axis=1, inplace=True)


# Attach postcode data to houses
hp_curr = hpf.loc[hpf.CHANGEIDEND == 0]
hp_hist = hpf.loc[hpf.CHANGEIDEND != 0]
only_historic = hp_hist.loc[~hp_hist.OBJECTID.isin(hp_curr.OBJECTID)].copy()
only_historic.CHANGEIDEND = 0
only_historic.VALUE = ''
no_pc_info = hf.loc[~hf.OBJECTID.isin(hpf.OBJECTID)].copy()
no_pc_info['CHANGEIDEND'] = 0
no_pc_info['TYPEID'] = 5
no_pc_info['VALUE'] = ''
no_pc_info = no_pc_info[hp.columns]
allpcs = hp_curr.append(hp_hist).append(only_historic).append(no_pc_info)
hpc = pd.merge(hf, allpcs, how='left', on='OBJECTID')

# Format houses
hpc['BUILDNUM'] = hpc['ADDNUM1']
hpc['STRUCNUM'] = hpc['ADDNUM2']
hpc['POSTALCODE'] = hpc['VALUE'].fillna('').astype(str)
hpc['CURRENT'] = 0
hpc.loc[hpc.CHANGEIDEND == 0, 'CURRENT'] = 1
hr = hpc[['OBJECTGUID', 'CURRENT', 'POSTALCODE', 'HOUSENUM', 'BUILDNUM',
          'STRUCNUM', 'OBJECTID']].copy()

# Attach parent information
# Iteration 0 - setup
hr.rename(columns={'OBJECTID': 'px'}, inplace=True)
mhfx = mhf[['OBJECTID', 'PARENTOBJID']].copy()
p = hr.copy()

for i in range(1, 7):
    print(f'Iteration {i}')
    p = p.merge(mhfx, how='left', left_on='px', right_on='OBJECTID')    
    p = p.drop('OBJECTID', axis=1)
    p = p.merge(aof, how='left', left_on='PARENTOBJID', right_on='OBJECTID')
    p = p.rename(columns={'px':f'p{i - 1}'})
    p = p.rename(columns={'NAME':f'n{i}', 'TYPENAME':f't{i}', 'LEVEL':f'l{i}'})
    p = p.drop('PARENTOBJID', axis=1)
    p = p.rename(columns={'OBJECTID':'px'})

assert p.l6.max() == 1  # i.e. the top level is `region`

# Distribute the levels correctly
# Setup
r = p[hr.columns].copy()
r.drop('px', axis=1, inplace=True)
levels = ['STREET', 'TERR', 'PLACE', 'CITY', 'MUNI', 'MUNR', 'ADMR', 'REGION']
for level in levels:
    r[f'{level}_S'] = ''
    r[f'{level}_F'] = ''

# Fill out
for idx, level in enumerate(levels[::-1], 1):
    for col in range(1, 7):
        r.loc[p[f'l{col}'] == idx, f'{level}_S'] = p[f't{col}']
        r.loc[p[f'l{col}'] == idx, f'{level}_F'] = p[f'n{col}']

assert list(r.ADMR_S.unique()) == ['']
assert list(r.ADMR_F.unique()) == ['']

# Save the oblast file
r.to_csv('Adygea.csv', sep=';', index=False)
































