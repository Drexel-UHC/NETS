# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 14:33:42 2022

@author: stf45

this is a temporary script to convert NETS_coded_sample wide into long. working
code will eventually be added to the end of classify.py.
"""


import pandas as pd
import time
import json
import nets_functions as nf
import warnings 
from datetime import datetime

#%% LOAD JSON CONFIG

with open('../config/json_config_2022_04_20_MAR.json', 'r') as f:
    config = json.load(f)
#%% READ FILE

n = 7681
chunksize = 100
cats = list(config.keys())
cats.insert(0,'DunsYear')
coded_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_coded_sample.txt', sep = '\t', dtype={"DunsNumber": str, "DunsYear": str}, encoding_errors='replace', header=0, 
                             chunksize=chunksize,
                             index_col=0,
                             usecols=cats)

#%% CREATE CATEGORY/CODE TABLE

cat_table = pd.DataFrame({'cat_name':config.keys(), 'cat_id': pd.Series(range(len(config)))})

new_df = pd.DataFrame(columns=['category1', 'category2', 'category3', 'category4'])

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, x in enumerate(coded_reader):
    header = (c==0)
    x['category1'] = x.loc[x.values == x.values.max()]
    x.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/NETS_recode_sample.txt", sep="\t", header=header, mode='a')
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

recode = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_recode_sample.txt', sep='\t', dtype = object, nrows=30000)
