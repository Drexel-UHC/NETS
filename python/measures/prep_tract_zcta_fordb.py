# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:03:48 2023

@author: stf45
"""

import pandas as pd

#%% LOAD TRACT FILE

tract = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\Tract_ZCTA_Hierarchy\BEDDN_t10_measures_hier20240506.txt', sep='\t', dtype={'tract10':str})

#%% CREATE UNIQUE INTEGER IDS FOR TRACT AND TRACT-YEAR

tractcopy = tract[['tract10','Year']]
tractcopy['TractYearId'] = range(len(tractcopy))
tract10id = pd.DataFrame(tractcopy['tract10'].unique(),columns=['tract10'])
tract10id['Tract10Id'] = range(len(tract10id))
tract10id = tract10id[['Tract10Id', 'tract10']]
tract2 = tractcopy.merge(tract10id, how='left', on='tract10')
tract2 = tract2.merge(tract, on=['tract10','Year'])

#%% CREATE COLUMN LISTS FOR COUNTS AND DENSITIES DATASETS
tractcols = list(tract.columns)

counts=[]
dens=[]

for col in tractcols:
    if col[-1] == 'c':
        counts.append(col)
    elif col[-1] == 'd':
        dens.append(col)
    else:
        pass

startcols = ['TractYearId', 'Tract10Id', 'Year']
counts[0:0] = startcols
dens[0:0] = startcols

#%% CREATE COUNTS AND TRACT ID TABLES AND EXPORT 

# export counts table
tractcounts = tract2[counts]
tractcounts.to_csv(r'D:\scratch\BEDDN_t10_counts_hierYYYYMMDD.txt', sep='\t', index=False)

# export tract id table. DONT NEED FOR HIERACHY, IDS ARE SAME AS NON HIERARCHY
# tract10id.to_csv(r'D:\scratch\Tracts.txt', sep='\t', index=False)

#%% CREATE DENSITIES TABLE AND EXPORT

# round float columns to 2 decimals
tractdens = tract2[dens]
datacols = dens[3:]
tractdens[datacols] = tractdens[datacols].round(2)
tractdenshead = tractdens.head(10)
tractdens.to_csv(r'D:\scratch\BEDDN_t10_densities_hierYYYYMMDD.txt', sep='\t', index=False)

del tract, tractcounts, tractdens



##################################################
#   ZCTAS
##################################################
#%% LOAD ZCTA FILE

zcta = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\Tract_ZCTA_Hierarchy\BEDDN_z10_measures_hier20240506.txt', sep='\t', dtype={'zcta10':str})

#%% CREATE ZCTA10ID AND ZCTAYEARID

zctacopy = zcta[['zcta10','Year']]
zctacopy['ZCTAYearId'] = range(len(zctacopy))
zcta10id = pd.DataFrame(zctacopy['zcta10'].unique(),columns=['zcta10'])
zcta10id['ZCTA10Id'] = range(len(zcta10id))
zcta10id = zcta10id[['ZCTA10Id', 'zcta10']]
zcta2 = zctacopy.merge(zcta10id, how='left', on='zcta10')
zcta2 = zcta2.merge(zcta, on=['zcta10','Year'])

#%% CREATE COLUMN LISTS FOR COUNTS AND DENSITIES DATASETS
zcols = list(zcta.columns)

counts=[]
dens=[]

for col in zcols:
    if col[-1] == 'c':
        counts.append(col)
    elif col[-1] == 'd':
        dens.append(col)
    else:
        pass

startcols = ['ZCTAYearId', 'ZCTA10Id', 'Year']
counts[0:0] = startcols
dens[0:0] = startcols

#%% CREATE COUNTS AND ZCTA ID TABLES AND EXPORT

# export ZCTA id table. DONT NEED FOR HIERACHY, IDS ARE SAME AS NON HIERARCHY
# zcta10id.to_csv(r'D:\scratch\ZCTAsYYYYMMDD.txt', sep='\t', index=False)

zcounts = zcta2[counts]
zcounts.to_csv(r'D:\scratch\BEDDN_z10_counts_hierYYYYMMDD.txt', sep='\t', index=False)

#%% CREATE DENSITIES TABLE AND EXPORT

# round float columns to 2 decimals
zdens = zcta2[dens]
datacols = dens[3:]
zdens[datacols] = zdens[datacols].round(2)
zdenshead = zdens.head()

zdens.to_csv(r'D:\scratch\BEDDN_z10_densities_hierYYYYMMDD.txt', sep='\t', index=False)
