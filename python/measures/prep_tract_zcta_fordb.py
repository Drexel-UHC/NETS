# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:03:48 2023

@author: stf45
"""

import pandas as pd

#%% LOAD TRACT FILE

tract = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\NETS_tr10_measures20231207.txt', sep='\t', dtype={'tract10':str})

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

startcols = ['tract10','Year']
counts[0:0] = startcols
dens[0:0] = startcols

#%% CREATE COUNTS TABLE AND EXPORT

tractcounts = tract[counts]
tractcounts.to_csv(r'D:\scratch\NETS_tr10_countsYYYYMMDD.txt', sep='\t', index=False)

#%% CREATE DENSITIES TABLE AND EXPORT

# round float columns to 2 decimals
tractdens = tract[dens]
datacols = dens[2:]
tractdens[datacols] = tractdens[datacols].round(2)

tractdens.to_csv(r'D:\scratch\NETS_tr10_densitiesYYYYMMDD.txt', sep='\t', index=False)

del tract, tractcounts, tractdens

##################################################
#   ZCTAS
##################################################
#%% LOAD ZCTA FILE

zcta = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\NETS_z10_measures20231208.txt', sep='\t', dtype={'zcta10':str})

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

startcols = ['zcta10','Year']
counts[0:0] = startcols
dens[0:0] = startcols

#%% CREATE COUNTS TABLE AND EXPORT

zcounts = zcta[counts]
zcounts.to_csv(r'D:\scratch\NETS_z10_countsYYYYMMDD.txt', sep='\t', index=False)

#%% CREATE DENSITIES TABLE AND EXPORT

# round float columns to 2 decimals
zdens = zcta[dens]
datacols = dens[2:]
zdens[datacols] = zdens[datacols].round(2)

zdens.to_csv(r'D:\scratch\NETS_z10_densitiesYYYYMMDD.txt', sep='\t', index=False)


