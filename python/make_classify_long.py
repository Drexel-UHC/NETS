# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 14:33:42 2022

@author: stf45

this is a temporary script to convert NETS_coded_sample wide into long. working
code will eventually be added to the end of classify.py? ::no just keep em separate. 
"""

import pandas as pd
import time
import json
from datetime import datetime

#%% LOAD JSON CONFIG

with open('../config/json_config_2022_04_20_MAR.json', 'r') as f:
    config = json.load(f)
    
#%% READ FILE

n = 7681
chunksize = 1000
cats = list(config.keys())
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
    x = pd.melt(x, id_vars=['DunsYear'])
    x = x.loc[x['value']==1]
    x = x.drop(columns=['value'])
    x.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_long_sample.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% DATA CHECK
recode = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_long_sample.txt', sep='\t', dtype = object, nrows=30000)

# check dunsyears with multiple cats
dups = recode.loc[recode.duplicated(subset=['DunsYear'], keep=False)]
