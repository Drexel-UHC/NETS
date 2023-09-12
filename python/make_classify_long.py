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
    
# create list of columns that includes all columns other than those 
cats = []
for cat in config.keys():
    if config[cat]['conditional'] not in [14]:
        cats.append(cat)
    else: pass
cats.insert(0,'DunsYear')

# load reader
n = 254288461
chunksize = 5000000
coded_reader = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classified_python20230410.txt', sep = '\t', encoding_errors='replace', header=0, 
                              # chunksize=chunksize,
                              # usecols=cats,
                              nrows=10)

#%% CREATE CATEGORY/CODE TABLE

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

