# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 13:20:42 2023

@author: stf45
"""

import pandas as pd
import time
from datetime import datetime

#%%

classified = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231024.txt', sep='\t', usecols = ['DunsYear'])
dunsyears = set(classified['DunsYear']) #303,014,511 unique dunsyears
del classified

 #%% FILTER classification.txt FOR SICS IN SET

chunksize=60000000
n=564824373
bi_reader = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData/classification_input20231016.txt', sep='\t', header=0, dtype={'DunsNumber':str, 'SIC':str},
                                chunksize=chunksize,
                                # nrows=100
                              )
#%%
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

rownum=[]
for c,bi in enumerate(bi_reader):
    header = (c==0)
    # subset for records with classified dunsyear
    bi = bi.loc[bi['DunsYear'].isin(dunsyears)]
    # remove whitespace from Company, TradeName columns
    bi[['Company', 'TradeName']] = bi[['Company', 'TradeName']].apply(lambda x: x.str.strip()) 
    #rearraging columns to match database
    bi = bi[['DunsYear','DunsNumber','Year','Company','TradeName', 'Emp','Sales','SIC']]
    rownum.append(len(bi))
    bi.to_csv(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/BusinessInfoYYYYMMDD.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"BusinessInfoYYYYMMDD n = {sum(rownum)}")

#%% 


df = pd.read_csv(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/BusinessInfoYYYYMMDD.txt', sep="\t", dtype={'DunsNumber':str, 'SIC':str})


