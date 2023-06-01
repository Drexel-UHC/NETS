# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 14:33:42 2022

@author: stf45

This is a script to convert the Classified Dataset (Python) (classifiedYYYYMMDD.txt) from wide to long. 


runtime: ~80 mins
"""
#%% 
import pandas as pd
import time
import json
from datetime import datetime
    
#%% READ CLASSIFIED DATASET (PYTHON) FILE

# load in json config
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230329.json', 'r') as f:
    config = json.load(f)
    
    
n = 254288461
chunksize = 5000000
cats = []
for cat in config.keys():
    if config[cat]['conditional'] not in [14]:
        cats.append(cat)
    else: pass
cats.insert(0,'DunsYear')
coded_reader = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classified20230410.txt', sep = '\t', encoding_errors='replace', header=0, 
                              chunksize=chunksize,
                              usecols=cats)

#%% CREATE CATEGORY/CODE TABLE

# cat_table = pd.DataFrame({'cat_name':config.keys(), 'cat_id': pd.Series(range(len(config)))})
##this is probably not necessary^^^

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, x in enumerate(coded_reader):
    header = (c==0)
    x = pd.melt(x, id_vars=['DunsYear'], var_name='BaseGroup')
    x = x.loc[x['value']==1]
    x = x.drop(columns=['value'])
    x.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_longYYYYMMDD.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% DATA CHECK

#CHECK TO SEE IF N OF CLASSIFED == N OF CLASSIFICATION SIC SUBSET 

classified_long = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_longYYYYMMDD.txt', sep='\t', dtype = object, nrows=30000)
dups = classified_long.loc[classified_long.duplicated(subset=['DunsYear'], keep=False)]
print(classified_long.nunique())

# what variables are missing from config (no records flagged)
print(f"Base Groups not flagged: {set(config.keys()).difference(set(classified_long['BaseGroup'].unique()))}")


