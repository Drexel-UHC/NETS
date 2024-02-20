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
dunslocs = pd.read_csv(dunslocs_file, usecols=['AddressID', 'ZCTA5CE10'], sep='\t', dtype={'ZCTA5CE10':str}) #n=25,959,371

# merge dunsmove and dunslocation, then delete the pre-merge files
movelocs = dunsmove.merge(dunslocs[['AddressID', 'ZCTA5CE10']], left_on='AddressID', right_on='AddressID').drop(columns=['AddressID']) #300,451,306
del dunsmove, dunslocs

#%% LOAD FILES, MERGE 2

#load classifiedlong file, merge to movelocs, delete classifiedlong
cl = pd.read_csv(cl_file, sep='\t', usecols=['DunsYear','BaseGroup']) #n=314,233,817
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
del zctacounts

# # save basegroupwide to file just in case
basegroupwide.to_csv(r'D:scratch\zctabasegroupwide.txt', sep='\t', index=False)

del basegroupwide


###############################################################################
### GET HIGH LEVEL COUNTS
###############################################################################



#%% GET HIGH LEVEL COUNTS

# load dunsmove key and dunslocations files
dunsmove = pd.read_csv(dunsmove_file, usecols=['AddressID', 'DunsYear','Year'], sep='\t') #n=300,476,419
print(len(dunsmove))
dunslocs = pd.read_csv(dunslocs_file, usecols=['AddressID', 'ZCTA5CE10'], sep='\t', dtype={'ZCTA5CE10':str}) #n=25,959,371
print(len(dunslocs))

# merge dunsmove and dunslocation, then delete the pre-merge files
movelocs = dunsmove.merge(dunslocs[['AddressID', 'ZCTA5CE10']], left_on='AddressID', right_on='AddressID').drop(columns=['AddressID']) #300,451,306
print(len(movelocs))
del dunsmove, dunslocs

#load classifiedlong file, merge to movelocs, delete classifiedlong
cl = pd.read_csv(cl_file, sep='\t', usecols=['DunsYear','BaseGroup']) #n=314,233,817
print('cl:',len(cl))
merged = cl.merge(movelocs) # n=311,611,271 
print('merged:',len(merged))
del cl, movelocs

# merged will have a smaller rowcount than cl because businesses outside of the 
##contiguous US were classified, but won't be further processed.

#%% SPLIT MERGED INTO SMALLER FILES (BECAUSE OF MEMORY LIMITATIONS)

# these year ranges will need to change if NETS vintage is > 2024
for year in range(1990,2022,5):    
    print(year)
    mergedsubset = merged.loc[(merged['Year'] >= year) & (merged['Year'] <= year+4)]
    mergedsubset.to_csv(rf'D:\scratch\merged{year}_{year+4}.txt', sep='\t', index=False)
    del mergedsubset

#%% GET HIGH LEVEL CATEGORY COUNTS IN CHUNKS BY 5 YEAR RANGE. EXPORT TO WIDE FILES IN 5YR.

xwalk = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\\BG_CC_TC_Xwalk20231023.txt', sep='\t', usecols=['BaseGroup', 'HighLevel'])

# these year ranges will need to change if NETS vintage is > 2024
for year in range(1990,2022,5):
    print(year)
    merged = pd.read_csv(rf'D:\scratch\merged{year}_{year+4}.txt', sep='\t', dtype={'ZCTA5CE10':str})
    # merge xwalk into merged to get high level categories matched to each base group.
    merged2 = merged.merge(xwalk).drop(columns='BaseGroup')
    # merged2 will be fewer records because base groups that do not fall into higher
    #level cats will be dropped.
    del merged
    # drop duplicates of dunsyear-highlevel, so none are double counted due to 
    #a dunsyear being in more than 1 basegroup that feeds into a high level category.
    merged3 = merged2.drop_duplicates(subset=['DunsYear', 'Year', 'HighLevel'], keep='last') 
    del merged2
    # groupby zcta, year, and highlevel and sum count column to get counts of highlevel
    #categories by year and zcta'
    highlevelzctacounts = pd.DataFrame(merged3.groupby(['ZCTA5CE10','Year'])['HighLevel'].value_counts())
    del merged3
    highlevelzctacounts = highlevelzctacounts.rename(columns={'HighLevel':'Count'}).reset_index() #n= 116,217,583
    print(f'len of highlevelzctacounts {year}: {len(highlevelzctacounts)}')
    highlevelzctacounts2 = pd.DataFrame(highlevelzctacounts.groupby(['ZCTA5CE10','Year','HighLevel'])['Count'].sum()) #n=97,169,232
    print(f'len of highlevelzctacounts2 {year}: {len(highlevelzctacounts2)}')
    del highlevelzctacounts
    highlevelzctacounts2 = highlevelzctacounts2.reset_index() 
    print(f"number of unique high level cats. should be 71:{highlevelzctacounts2['HighLevel'].nunique()}") #number of unique HighLevel Categories: n=71
    
    # pivot to get wide version: counts by zcta, year, and highlevel category
    highlevelwide = pd.pivot(highlevelzctacounts2, index=['ZCTA5CE10','Year'], columns=['HighLevel'], values='Count') #n=2,377,263 (do some basegroups not belong to a highlevel cat? yes)
    highlevelwide = highlevelwide.reset_index()
    highlevelwide.to_csv(rf'D:\scratch\highlevelwide{year}_{year+4}.txt', sep='\t', index=False)

del highlevelzctacounts2

#%% MERGE SEPERATE HIGHLEVELWIDE FILES TO BASEGROUPWIDE TO CREATE FULLWIDE

basegroupwide = pd.read_csv(r'D:/scratch/zctabasegroupwide.txt', sep='\t', dtype={'ZCTA5CE10':str})
fullwide = pd.DataFrame()

# these year ranges will need to change if NETS vintage is > 2024
for year in range(1990,2022,5): 
    print(year)
    basegroupwidetemp = basegroupwide.loc[(basegroupwide['Year'] >= year) & (basegroupwide['Year'] <= year+4)]
    highlevelwide = pd.read_csv(rf'D:\scratch\highlevelwide{year}_{year+4}.txt', sep='\t', dtype={'ZCTA5CE10':str})
    fullwidetemp = basegroupwidetemp.merge(highlevelwide, on=['ZCTA5CE10','Year'], how='left')
    fullwide = pd.concat([fullwide,fullwidetemp])

fullwidehead = fullwide.head()

#%% REARRANGE COLUMNS

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

fullwide = fullwide.sort_values(by=['Year','zcta10'])
fullwidehead = fullwide.head(100)

#%% CHECK SOME EXAMPLES TO SEE IF HIGHERLEVEL CATS ARE SUMS OF THEIR BASEGROUPS

walkwide = xwalk.groupby('HighLevel')['BaseGroup'].unique().reset_index()

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
    
# all true as of 12/08/2023

#%% GET DENSITIES

dunslocs = pd.read_csv(dunslocs_file, usecols=['AddressID', 'ZCTA5CE10', 'ZCTALandArea'], sep='\t', dtype={'ZCTA5CE10':str}) #n=25,959,371

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


#%% SUBSET FOR PHILLY 2010, 2022 FOR DATACHECK

metrozcta = pd.read_csv(r'Z:\UHC_Data\MetroPHL\tblZCTA10_MetroPHL.csv', dtype={'ZCTA5CE10':str}, usecols=['ZCTA5CE10', 'PHL'])
phlzcta = metrozcta['ZCTA5CE10'].loc[metrozcta['PHL']==1]


# subset philly county and columns recommended by steve (food & communities)
phillywide = fullwide.loc[fullwide['zcta10'].isin(phlzcta)]
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

phillywide10 = phillywide.loc[phillywide['Year'] == 2010]
phillywide22 = phillywide.loc[phillywide['Year'] == 2022]

#%% EXPORT TO CSV

phillywide10.to_csv(r'D:\scratch\NETS_z10_phl2010_YYYYMMDD.txt', sep='\t', index=False)
phillywide22.to_csv(r'D:\scratch\NETS_z10_phl2022_YYYYMMDD.txt', sep='\t', index=False)

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

df2 = zctayear.merge(fullwide, how='left', left_on=['ZCTA5CE10','Year'], right_on=['zcta10','Year']).drop(columns='zcta10')

# replace nans with 0s
df2 = df2.fillna(0)

df2 = df2.rename(columns={'ZCTA5CE10':'zcta10'})
df2head = df2.head(50)

#%% EXPORT TO CSV

df2.to_csv(r'D:\scratch\NETS_z10_measuresYYYYMMDD.txt', sep='\t', index=False)





