# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 14:34:43 2023

@author: stf45
"""

import pandas as pd

#%% LOAD FILE 
usecols = ['AddressID',
            'Xcoord',
            'Ycoord',
            'DisplayX',
            'DisplayY',
            'GEOID10',
            'TractLandArea',
            'TractTotalArea',
            'ct10_distance',
            'ZCTA5CE10',
            'ZCTALandArea',
            'ZCTATotalArea',
            'zcta10_distance',
            'Addr_type',
            'Status',
            'Score',
            'UHCMatchCodeRank']

df = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\DunsLocation20231207.txt', sep='\t', dtype={'GEOID10':str, 'ZCTA5CE10':str}, 
                 usecols=usecols) # n=25,963,768
df = df[usecols]
df = df.reset_index().rename(columns={'index':'DunsLocationId'})

#%% HOW MANY RECORDS WERE NOT GEOCODED?:: 4397
check = df.loc[df['UHCMatchCodeRank'].isna()]
print(len(check))

check = df.loc[df['DisplayY'].isna()]
print(len(check))

#%% REMOVE NON-GEOCODED RECORDS

df = df.loc[df['DisplayY'].notna()] # n=25,959,371

#%% CONVERT AREA MEASURES TO KM

df['TractLandArea'] = df['TractLandArea'] / 1000000
df['TractTotalArea'] = df['TractTotalArea'] / 1000000
df['ZCTALandArea'] = df['ZCTALandArea'] / 1000000
df['ZCTATotalArea'] = df['ZCTATotalArea'] / 1000000

#%% SET GEOID == NULL WHERE ct10_distance > 1000

# export csv of records where ct10_distance is > 1000 to check.
farfromtract = df.loc[df['ct10_distance']>1000]
farfromtract.to_csv(r'D:\scratch\dunslocs_farfromtract.csv', index=False)

# set geoid10 to None where ct10_distance is >1000. check to see if it worked.
df.loc[df['ct10_distance'] > 1000, 'GEOID10'] = None
check = df.loc[df['ct10_distance']>1000]

#%% SET ZCTA5CE10 == NULL WHERE zcta10_distance > 1000

# export csv of records where zcta_distance is > 1000 to check.
farfromtract = df.loc[df['zcta10_distance']>1000]
farfromtract.to_csv(r'D:\scratch\dunslocs_farfromzcta.csv', index=False)

# set geoid10 to None where zcta10_distance is >1000. check to see if it worked.
df.loc[df['zcta10_distance'] > 1000, 'ZCTA5CE10'] = None
check = df.loc[df['zcta10_distance']>1000] # these records include all 18 that were not matched to census tracts above.

#%% EXPORT TO CSV

df.to_csv(r'D:\scratch\DunsLocationYYYYMMDD.txt', sep='\t', index=False)

#%%
