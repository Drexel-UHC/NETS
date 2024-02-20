# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 09:10:41 2023

@author: stf45
"""

import pandas as pd

#%% READ IN TRACT LEVEL MEASURES FILE

df = pd.read_csv(r'D:\scratch\NETS_tr10_measuresYYYYMMDD.txt', sep='\t', dtype={'tract10':str})

# load file with all census tracts in the contiguous us.
alltracts10 = pd.read_csv(r'Z:\UHC_Data\Census2010\Geodatabases\tblCT_2010_ContUSLandRepairAlbers.csv', usecols=['GEOID10'], dtype={'GEOID10':str})

#%% CREATE DF OF ALL TRACTS IN ALL YEARS

tractyear = pd.DataFrame()
years = range(1990,2023)
for year in years:
    newdf = alltracts10.copy()
    newdf['Year'] = year
    tractyear = pd.concat([tractyear,newdf])

#%% MERGE TRACT MEASURES TO ALL TRACTS ALL YEARS TO ADD TRACT-YEARS WITH NO VALUES THAT WERE DROPPED

df2 = tractyear.merge(df, how='left', left_on=['GEOID10','Year'], right_on=['tract10','Year']) # n=2,384,118

# replace nans with 0s
df2 = df2.fillna(0)

df2 = df2.drop(columns='tract10')
df2 = df2.rename(columns={'GEOID10':'tract10'})
df2head = df2.head()

# export to csv
df2.to_csv(r'D:\scratch\NETS_tr10_measuresYYYYMMDD.txt', sep='\t', index=False)
