# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 13:20:42 2023

@author: stf45
"""

import pandas as pd
import time
from datetime import datetime

#%%

classified = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\ClassifiedLong20230526.txt', sep='\t', usecols = ['DunsYear'])
dunsyears = set(classified['DunsYear'])
del classified

 #%% FILTER classification.txt FOR SICS IN SET

chunksize=60000000
n=564824373
bi_reader = pd.read_csv(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classification_inputYYYYMMDD.txt', sep='\t', header=0, dtype={'DunsNumber':str, 'SIC':str},
                                chunksize=chunksize,
                               # nrows=100000
                              )

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

rownum=[]
for c,bi in enumerate(bi_reader):
    header = (c==0)
    # subset for reocrds with classified dunsyear
    bi = bi.loc[bi['DunsYear'].isin(dunsyears)]
    # remove whitespace from Company, TradeName columns
    bi[['Company', 'TradeName']] = bi[['Company', 'TradeName']].apply(lambda x: x.str.strip()) 
    rownum.append(len(bi))
    bi.to_csv(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/BusinessInfoYYYYMMDD.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"BusinessInfoYYYYMMDD n = {sum(rownum)}")

 #%% FILTER classification.txt FOR SICS IN SET

chunksize=60000000
n=275000000
bi_reader = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BusinessInfoDB20230802.txt', sep='\t', header=0, dtype={'DunsNumber':str, 'SIC':str},
                                chunksize=chunksize,
                                # nrows=100000
                              )

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

rownum=[]
for c,bi in enumerate(bi_reader):
    header = (c==0)
    bi = bi[['DunsYear','DunsNumber','Year','Company','TradeName', 'Emp','Sales','SIC']]
    bi.to_csv(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/BusinessInfoYYYYMMDD.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"BusinessInfoYYYYMMDD n = {sum(rownum)}")




