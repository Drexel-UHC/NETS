# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 12:11:47 2023

@author: stf45

This script generates NETS category counts per census tract-year. It creates 
wide and long versions, output to tab-delimited text files.
"""
 
import pandas as pd
import numpy as np

#%% LOAD FILES, MERGE

# SAMPLES
# cl_file = r'D:\NETS\NETS_2019\ProcessedData\dbsamples\ClassifiedLongDBsample.txt'
# dunsmove_file = r'D:\NETS\NETS_2019\ProcessedData\dbsamples\DunsMove_DunsYear_KeyDBsample.txt'
# dunslocs_file = r'D:\NETS\NETS_2019\ProcessedData\dbsamples\DunsLocationsDBsample.txt'
# dlsep='\t'
# right_on = 'AddressID'
# dropcols = ['AddressID']

# FULL FILES
cl_file = r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231127.txt'
dunsmove_file = r'D:\NETS\NETS_2022\ProcessedData\DunsMove20231201.txt'
dunslocs_file = r'D:\NETS\NETS_2022\ProcessedData\DunsLocation20231207.txt'

# load dunsmove key and dunslocations files
dunsmove = pd.read_csv(dunsmove_file, usecols=['AddressID', 'DunsYear','Year'], sep='\t') #n=300,476,419
dunslocs = pd.read_csv(dunslocs_file, usecols=['AddressID', 'GEOID10', 'TractLandArea'], sep='\t', dtype={'GEOID10':str}) #n=25,959,371

# merge dunsmove and dunslocation, then delete the pre-merge files
movelocs = dunsmove.merge(dunslocs[['AddressID', 'GEOID10']], left_on='AddressID', right_on='AddressID').drop(columns=['AddressID']) #300,451,306
del dunsmove

#%% LOAD FILES, MERGE 2

#load classifiedlong file, merge to movelocs, delete classifiedlong
cl = pd.read_csv(cl_file, sep='\t') #n=314,233,817
print('cl:',len(cl))
merged = cl.merge(movelocs) # n=311,611,271 
print('merged:',len(merged))
del cl, movelocs

#%% GET TRACT BASEGROUP COUNTS, PIVOT TO WIDE

# get counts of basegroups per tract-year
tractcounts = pd.DataFrame(merged.groupby(['GEOID10','Year'])['BaseGroup'].value_counts())
del merged
tractcounts = tractcounts.rename(columns={'BaseGroup':'Count'}).reset_index() #n= 116,217,583

# pivot to wide. option to export here if you just want basegroup counts by tract-year
basegroupwide = pd.pivot(tractcounts, index=['GEOID10','Year'], columns=['BaseGroup'], values='Count') #n= 2,377,534
basegroupwide = basegroupwide.reset_index()
basegroupwidehead = basegroupwide.head(100)

#%%  LOAD XWALK, JOIN TO TRACTCOUNTS, GET TRACT HIGHLEVEL COUNTS  

xwalk = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\\BG_CC_TC_Xwalk20231023.txt', sep='\t')
walkwide = xwalk.groupby('HighLevel')['BaseGroup'].unique().reset_index()

# merge xwalk into tractcounts to get high level categories. drop basegroup
#then groupby tract, year, and highlevel and sum count column to get counts of highlevel
#categories by year and tract
tractcounts = tractcounts.merge(xwalk) #n=355,382,528
tractcounts = tractcounts.drop(columns=['BaseGroup'])
tractcounts = pd.DataFrame(tractcounts.groupby(['GEOID10','Year','HighLevel'])['Count'].sum()) #n=97,169,232
tractcounts = tractcounts.reset_index() 
tracthead = tractcounts.head(100)
tractcounts['HighLevel'].nunique() #number of unique HighLevel Categories: n=71

# pivot to get wide version: counts by tract, year, and highlevel category
highlevelwide = pd.pivot(tractcounts, index=['GEOID10','Year'], columns=['HighLevel'], values='Count') #n=2,377,263 (do some basegroups not belong to a highlevel cat?)
highlevelwide = highlevelwide.reset_index()
highlevelwidehead = highlevelwide.head(100)

#%% MERGE BASEGROUPWIDE AND HIGHLEVELWIDE

highlevelwide['GEOID10'] = highlevelwide['GEOID10'].astype(str).str.zfill(11)
fullwide = basegroupwide.merge(highlevelwide, on=['GEOID10','Year'], how='outer')

# rename columns
fullwide = fullwide.rename(columns={'GEOID10': 'tract10'})
keepcols = ['tract10','Year']
cols = [f't10_net_{col.lower()}_c' for col in fullwide.columns if col not in keepcols]
colsort= cols.copy()
cols.insert(0,'Year')
cols.insert(0,'tract10')
fullwide.columns = cols

# reorder columns alphabetically
colsort.sort()
colsort.insert(0,'Year')
colsort.insert(0,'tract10')
fullwide = fullwide[colsort]

fullwidehead = fullwide.head(100)

#%% CHECK SOME EXAMPLES TO SEE IF HIGHERLEVEL CATS ARE SUMS OF THEIR BASEGROUPS

for hl in walkwide['HighLevel']:
    # list of basegroup columns
    collist = list(walkwide['BaseGroup'].loc[walkwide['HighLevel'] == hl].values[0])
    collist = [f't10_net_{col.lower()}_c' for col in collist]
    # highlevel column in question
    highlevelcol = fullwide[f't10_net_{hl.lower()}_c'].replace(np.nan,0)
    # sum basegroup cols
    sumofbasegroupcols = fullwide[collist].sum(axis=1)
    # equality check
    eqcheck = sumofbasegroupcols.equals(highlevelcol)
    print(f'{hl} column equals sum of basegroups: {eqcheck}')
    
# all true as of 12/05/2023

#%% GET DENSITIES

# make sure there are no duplicate tracts
tractareas = dunslocs[['GEOID10', 'TractLandArea']].drop_duplicates()
# merge counts with tract land area col
fullwide = fullwide.merge(tractareas, left_on='tract10', right_on='GEOID10', how="left").drop(columns=['GEOID10'])
fullwidefirstcols = ['tract10', 'Year', 'TractLandArea']
fullwidehead = fullwide.head(100)
fullwide = fullwide.fillna(0)

# create densities by dividing each column by land area. convert count columns from float to integer
for col in fullwide.columns:
    if col in fullwidefirstcols: 
        pass
    else:
        fullwide[col] = fullwide[col].astype(int)
        newcolname = col[:-1] + 'd'
        print(newcolname)
        fullwide[newcolname] = fullwide[col] / fullwide['TractLandArea']


# reorder columns alphabetically
colsort2 = list(fullwide.columns)
[colsort2.remove(col) for col in fullwidefirstcols]
colsort2.sort()
colsort2.insert(0,'Year')
colsort2.insert(0,'tract10')
fullwide = fullwide[colsort2]
fullwidehead = fullwide.head(100)

#%% SUBSET FOR PHILLY FOR DATACHECK

# subset philly county and columns recommended by steve 
phillywide = fullwide.loc[(fullwide['tract10'].str[:5]=='42101')]
phillywide = phillywide[['tract10', 
                         'Year', 
                         't10_net_bar_d',
                         't10_net_sma_d',
                         't10_net_hsr_d',
                         't10_net_cna_d',  
                         't10_net_ffa_d', 
                         't10_net_usr_d',
                         't10_net_aur_d', 
                         't10_net_dra_d', 
                         't10_net_cmu_d',
                         't10_net_cmp_d',
                         't10_net_cvp_d',
                         't10_net_dcr_d',
                         't10_net_scl_d',
                         't10_net_psc_d']]

#%% EXPORT TO CSV

phillywide.to_csv(r'D:\scratch\NETS_tr10_measures_phillyYYYYMMDD.txt', sep='\t', index=False)
fullwide.to_csv(r'D:\scratch\NETS_tr10_measuresYYYYMMDD.txt', sep='\t', index=False)

