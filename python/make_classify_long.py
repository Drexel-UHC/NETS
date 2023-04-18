# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 14:33:42 2022

@author: stf45

this is a script to convert classified.txt from wide into long. 


runtime:
"""

import pandas as pd
import time
import json
from datetime import datetime
    
#%% READ FILE

# load in json config
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230227.json', 'r') as f:
    config = json.load(f)
    
    
n = 3250
chunksize = 500
cats = []
for cat in config.keys():
    if config[cat]['conditional'] not in [14]:
        cats.append(cat)
    else: pass
cats.insert(0,'DunsYear')
coded_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified.txt', sep = '\t', dtype={"DunsYear": str}, encoding_errors='replace', header=0, 
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
    x.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_long.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% DATA CHECK

classified_long = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_long.txt', sep='\t', dtype = object, nrows=30000)
dups = classified_long.loc[classified_long.duplicated(subset=['DunsYear'], keep=False)]
print(classified_long.nunique())

# what variables are missing from config (no records flagged)
print(f"Base Groups not flagged: {set(config.keys()).difference(set(classified_long['BaseGroup'].unique()))}")


