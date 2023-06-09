# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This file creates the linkage between DunsYear and DunsMove by merging the classification input file and the 
geocoding input file on dunsnumber, and subsetting for where DunsMove year ranges match DunsYears.

runtime: ~1hour
"""

#%%

import pandas as pd
import time
from datetime import datetime

#%% READ IN DATA

# FULL FILES
chunksize = 10000000
n = 32967561
dunsmove = r'C:\Users\stf45\Documents\NETS\Processing\scratch\DunsMoveYYYYMMDD.txt'
classfile = r'C:\Users\stf45\Documents\NETS\Processing\scratch\ClassifiedLongYYYYMMDD.txt'
classified_long = pd.read_csv(classfile, sep = '\t', dtype={'DunsYear':str, 'DunsMove':str}, usecols = ['DunsYear'], header=0)
classified_long = classified_long.drop_duplicates(subset='DunsYear')

#%% CREATE DUNSYEAR DUNSMOVE KEY

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

dunsmove_reader = pd.read_csv(dunsmove, sep = '\t', dtype={'DunsNumber':str, 'DunsMove':str}, 
                          chunksize=chunksize,
                          header=0
                          )

classified_long['DunsNumber'] = classified_long['DunsYear'].str[:9]

yearmovelen = []
for c, chunk in enumerate(dunsmove_reader):
    header = (c==0)
    yearmove = chunk.merge(classified_long, on='DunsNumber', how='left')
    yearmove = yearmove.drop(columns=['DunsNumber'])
    yearmove = yearmove.loc[~yearmove['DunsYear'].isna()]
    yearmove['YearFull'] = yearmove['DunsYear'].str[-4:].astype(int)
    yearmove = yearmove.loc[(yearmove['YearFull'] >= yearmove['GcFirstYear']) & (yearmove['YearFull'] <= yearmove['GcLastYear'])]
    yearmove = yearmove[['DunsYear','DunsMove']]
    yearmove.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\DunsMove_DunsYear_KeyYYYYMMDD.txt', sep='\t', mode='a', index=False, header=header,)
    yearmovelen.append(len(yearmove))
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))    

print(f'DunsMove_DunsYear_KeyYYYYMMDD has {sum(yearmovelen)} records')

#%% DATACHECK

del chunk
del classified_long
yearmove = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\DunsMove_DunsYear_KeyYYYYMMDD.txt', sep = '\t', dtype=str, header=0)

dups = yearmove.loc[yearmove.duplicated(subset='DunsYear', keep=False)]
dupshead = dups.head(1000)

dy_unique = yearmove['DunsYear'].nunique()
dm_unique = yearmove['DunsMove'].nunique()
dn_unique = yearmove['DunsYear'].str[:9].nunique()

# check1 = yearmove.loc[yearmove['DunsYear'].str[:9] == '001017107']
# check11 = dunsmove_reader.loc[dunsmove_reader['DunsNumber'] == '001017107']


# check2019 = yearmove.loc[(yearmove['DunsYear'].str[:-4] == '2019')]

# lens = dunsmove_reader['GcAddress'].str.rstrip()
# lens = lens.str.len()
# long = lens.loc[lens == lens.max()].reset_index()
# longs = dunsmove_reader.iloc[long['index']]

# lens2 = georeader['GcCity'].str.rstrip()
# lens2 = lens2.str.len()
# long2 = lens2.loc[lens2 == lens2.max()].reset_index()
# longs2 = georeader.iloc[long2['index']]


