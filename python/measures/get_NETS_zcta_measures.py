# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 09:14:05 2023

@author: stf45

This script generates NETS category counts per census zcta-year. It creates 
wide and long versions, output to tab-delimited text files.
"""
 
import pandas as pd
import numpy as np

#%% LOAD FILES, MERGE

# FULL FILES
cl_file = r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231127.txt'
dunsmove_file = r'D:\NETS\NETS_2022\ProcessedData\DunsMove20231201.txt'
dunslocs_file = r'D:\NETS\NETS_2022\ProcessedData\DunsLocation20231207.txt'

# load dunsmove key and dunslocations files
dunsmove = pd.read_csv(dunsmove_file, usecols=['AddressID', 'DunsYear','Year'], sep='\t') #n=300,476,419
dunslocs = pd.read_csv(dunslocs_file, usecols=['AddressID', 'ZCTA5CE10', 'ZCTALandArea'], sep='\t', dtype={'ZCTA5CE10':str}) #n=25,959,371

# merge dunsmove and dunslocation, then delete the pre-merge files
movelocs = dunsmove.merge(dunslocs[['AddressID', 'ZCTA5CE10']], left_on='AddressID', right_on='AddressID').drop(columns=['AddressID']) #300,451,306
del dunsmove

#%% LOAD FILES, MERGE 2

#load classifiedlong file, merge to movelocs, delete classifiedlong
cl = pd.read_csv(cl_file, sep='\t') #n=314,233,817
print('cl:',len(cl))
merged = cl.merge(movelocs) # n=311,611,271 
print('merged:',len(merged))
del cl, movelocs

#%% GET ZCTA BASEGROUP COUNTS, PIVOT TO WIDE

# get counts of basegroups per zcta-year
zctacounts = pd.DataFrame(merged.groupby(['ZCTA5CE10','Year'])['BaseGroup'].value_counts())
del merged
zctacounts = zctacounts.rename(columns={'BaseGroup':'Count'}).reset_index() #n=55,920,576

# pivot to wide. option to export here if you just want basegroup counts by zcta-year
basegroupwide = pd.pivot(zctacounts, index=['ZCTA5CE10','Year'], columns=['BaseGroup'], values='Count') #n=1,063,042
basegroupwide = basegroupwide.reset_index()
basegroupwidehead = basegroupwide.head(100)

#%%  LOAD XWALK, JOIN TO ZCTACOUNTS, GET ZCTA HIGHLEVEL COUNTS  

xwalk = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\\BG_CC_TC_Xwalk20231023.txt', sep='\t')
walkwide = xwalk.groupby('HighLevel')['BaseGroup'].unique().reset_index()

# merge xwalk into zctacounts to get high level categories. drop basegroup
#then groupby zcta, year, and highlevel and sum count column to get counts of highlevel
#categories by year and zcta
zctacounts = zctacounts.merge(xwalk) #n=199,386,122
print('zctacounts:',len(zctacounts))
zctacounts = zctacounts.drop(columns=['BaseGroup'])
zctacounts = pd.DataFrame(zctacounts.groupby(['ZCTA5CE10','Year','HighLevel'])['Count'].sum()) #n=40,836,757
print('zctacounts grouped:',len(zctacounts))
zctacounts = zctacounts.reset_index() 
zctahead = zctacounts.head(100)
print(zctacounts['HighLevel'].nunique()) #number of unique HighLevel Categories: n=71

# pivot to get wide version: counts by zcta, year, and highlevel category
highlevelwide = pd.pivot(zctacounts, index=['ZCTA5CE10','Year'], columns=['HighLevel'], values='Count') #n=1,062,177 (do some basegroups not belong to a highlevel cat?)
highlevelwide = highlevelwide.reset_index()
highlevelwidehead = highlevelwide.head(100)
print('highlevelwide:',len(highlevelwide))

#%% MERGE BASEGROUPWIDE AND HIGHLEVELWIDE

highlevelwide['ZCTA5CE10'] = highlevelwide['ZCTA5CE10'].astype(str).str.zfill(5)
fullwide = basegroupwide.merge(highlevelwide, on=['ZCTA5CE10','Year'], how='outer')

# rename columns
fullwide = fullwide.rename(columns={'ZCTA5CE10': 'zcta10'})
keepcols = ['zcta10','Year']
cols = [f'z10_net_{col.lower()}_c' for col in fullwide.columns if col not in keepcols]
colsort= cols.copy()
cols.insert(0,'Year')
cols.insert(0,'zcta10')
fullwide.columns = cols

# reorder columns alphabetically
colsort.sort()
colsort.insert(0,'Year')
colsort.insert(0,'zcta10')
fullwide = fullwide[colsort]

fullwidehead = fullwide.head(100)

#%% CHECK SOME EXAMPLES TO SEE IF HIGHERLEVEL CATS ARE SUMS OF THEIR BASEGROUPS

for hl in walkwide['HighLevel']:
    # list of basegroup columns
    collist = list(walkwide['BaseGroup'].loc[walkwide['HighLevel'] == hl].values[0])
    collist = [f'z10_net_{col.lower()}_c' for col in collist]
    # highlevel column in question
    highlevelcol = fullwide[f'z10_net_{hl.lower()}_c'].replace(np.nan,0)
    # sum basegroup cols
    sumofbasegroupcols = fullwide[collist].sum(axis=1)
    # equality check
    eqcheck = sumofbasegroupcols.equals(highlevelcol)
    print(f'{hl} column equals sum of basegroups: {eqcheck}')
    
# all true as of 

#%% GET DENSITIES

# make sure there are no duplicate zctas
zctaareas = dunslocs[['ZCTA5CE10', 'ZCTALandArea']].drop_duplicates()
# merge counts with zcta land area col
fullwide = fullwide.merge(zctaareas, left_on='zcta10', right_on='ZCTA5CE10', how="left").drop(columns=['ZCTA5CE10'])
fullwidefirstcols = ['zcta10', 'Year', 'ZCTALandArea']
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
        fullwide[newcolname] = fullwide[col] / fullwide['ZCTALandArea']


# reorder columns alphabetically
colsort2 = list(fullwide.columns)
[colsort2.remove(col) for col in fullwidefirstcols]
colsort2.sort()
colsort2.insert(0,'Year')
colsort2.insert(0,'zcta10')
fullwide = fullwide[colsort2]
fullwidehead = fullwide.head(100)


#%% SUBSET FOR PHILLY FOR DATACHECK

metrozcta = pd.read_csv(r'Z:\UHC_Data\MetroPHL\tblZCTA10_MetroPHL.csv', dtype={'ZCTA5CE10':str}, usecols=['ZCTA5CE10', 'PHL'])
metrozcta = metrozcta.loc[metrozcta['PHL']==1]


# subset philly county and columns recommended by steve 
phillywide = fullwide.loc[fullwide['zcta10'].isin(metrozcta['ZCTA5CE10'])]
phillywide = phillywide[['zcta10', 
                         'Year', 
                         'z10_net_bar_d',
                         'z10_net_sma_d',
                         'z10_net_hsr_d',
                         'z10_net_cna_d',  
                         'z10_net_ffa_d', 
                         'z10_net_usr_d',
                         'z10_net_aur_d', 
                         'z10_net_dra_d', 
                         'z10_net_cmu_d',
                         'z10_net_cmp_d',
                         'z10_net_cvp_d',
                         'z10_net_dcr_d',
                         'z10_net_scl_d',
                         'z10_net_psc_d']]

#%% EXPORT TO CSV

phillywide.to_csv(r'D:\scratch\NETS_z10_measure_phillyYYYYMMDD.txt', sep='\t', index=False)

#%% BACKFILL MISSING ZCTAS

zcta_usa = pd.read_csv(r'Z:\UHC_Data\Census2010\Geodatabases\tblZCTA_2010_ContUS_intpt.csv', usecols=['ZCTA5CE10'], dtype={'ZCTA5CE10':str})

#%% CREATE DF OF ALL ZCTAS IN ALL YEARS

zctayear = pd.DataFrame()
years = range(1990,2023)
for year in years:
    newdf = zcta_usa.copy()
    newdf['Year'] = year
    zctayear = pd.concat([zctayear,newdf])

#%% MERGE ZCTA MEASURES TO ALL ZCTAS ALL YEARS TO ADD ZCTA-YEARS WITH NO VALUES THAT WERE DROPPED

df2 = zctayear.merge(fullwide, how='left', left_on=['ZCTA5CE10','Year'], right_on=['ZCTA5CE10','Year'])

# replace nans with 0s
df2 = df2.fillna(0)

df2 = df2.rename(columns={'ZCTA5CE10':'zcta10'})
df2head = df2.head()

#%% EXPORT TO CSV

fullwide.to_csv(r'D:\scratch\NETS_z10_measuresYYYYMMDD.txt', sep='\t', index=False)





