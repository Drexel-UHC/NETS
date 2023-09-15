# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 09:10:41 2023

@author: stf45
"""

import pandas as pd
from sas7bdat import SAS7BDAT

#%% READ IN TRACT LEVEL MEASURES FILE

df = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\NETS_tr10_measures20230823.txt', sep='\t', dtype={'tract10':str})

with SAS7BDAT(r"Z:\UHC_Data\Census2010\data\sf1_census2010_tract.sas7bdat") as file:
    alltracts10 = file.to_data_frame() 

with SAS7BDAT(r"Z:\UHC_Data\CensusACS15_19\data\tractchange_10_19.sas7bdat") as file:
    tractchange = file.to_data_frame() 

stevetracts = pd.read_csv(r'C:\Users\stf45\Downloads\missingtr10measures20230913.csv', dtype={'GEOID10':str})

#%% REMOVE NON-CONTIG AND ZERO LAND AREA TRACTS

alltracts10 = alltracts10[['GEOID10', 'AREALAND']]
noncontig = alltracts10.loc[~alltracts10['GEOID10'].str[:2].isin(['02', '15', '72'])]
noncontig = noncontig.loc[noncontig['AREALAND'] > 0]
noncontig = noncontig.drop(columns=['AREALAND'])

#%% CREATE DF OF ALL TRACTS IN ALL YEARS

tractyear = pd.DataFrame()
years = range(1990,2020)
for year in years:
    newdf = noncontig.copy()
    newdf['Year'] = year
    tractyear = pd.concat([tractyear,newdf])

#%% MERGE TRACT MEASURES TO ALL TRACTS ALL YEARS TO ADD TRACT-YEARS WITH NO VALUES THAT WERE DROPPED

df2 = tractyear.merge(df, how='left', left_on=['GEOID10','Year'], right_on=['tract10','Year'])

# replace nans with 0s
df2 = df2.fillna(0)

df2 = df2.drop(columns='tract10')
df2 = df2.rename(columns={'GEOID10':'tract10'})
df2.head()

# export to csv
df2.to_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\NETS_tr10_measures20230914.txt', sep='\t', index=False)

#%%

notin19.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\tracts2010_notin2019.csv', index=False)
notin10.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\tracts2019_notin2010.csv', index=False)
