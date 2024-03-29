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
dunsmove = r'D:\NETS\NETS_2022\ProcessedData\duns_move_1_20231027.txt'
classfile = r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231127.txt'
classified_long = pd.read_csv(classfile, sep = '\t', dtype={'DunsYear':str, 'DunsMove':str}, usecols = ['DunsYear'], header=0)
classified_long = classified_long.drop_duplicates(subset='DunsYear')

#%% CREATE DUNSMOVE

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
    yearmove = yearmove.loc[~yearmove['DunsYear'].isna()]
    yearmove['Year'] = yearmove['DunsYear'].str[-4:].astype(int)
    yearmove = yearmove.loc[(yearmove['Year'] >= yearmove['GcFirstYear']) & (yearmove['Year'] <= yearmove['GcLastYear'])]
    yearmove = yearmove[['DunsYear','DunsMove','DunsNumber','AddressID','Year']]
    yearmove.to_csv(r'D:\scratch\DunsMoveYYYYMMDD.txt', sep='\t', mode='a', index=False, header=header,)
    yearmovelen.append(len(yearmove))
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))    

print(f'DunsMoveYYYYMMDD has {sum(yearmovelen)} records')


