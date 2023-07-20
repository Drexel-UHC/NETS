# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 15:16:22 2023

@author: stf45

This file takes in classification_inputYYYYMMDD.txt and filters for SICs used in SAS processing.
"""


#%%

import os 
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import pandas as pd
from datetime import datetime
import time
import json
import nets_functions as nf

#%% LOAD CONFIG

# load in json config. this has all aux categories and their conditions.
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230329.json', 'r') as f:
    config = json.load(f)

#%% CREATE SET OF ALL SICS IN CONFIG (PYTHON PROCESSED)

sicset = set()

for c,cat in enumerate(config.keys()):
    if config[cat]['conditional'] in [14]:
        sics = nf.make_sic_range(cat,config)
        sicset.update(sics)
    else:
        pass
    


#%% FILTER classification.txt FOR SICS IN SET

chunksize=75000000
n=564824373
classification = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classification_inputYYYYMMDD.txt', sep='\t', header=0, dtype={'DunsNumber':str},
                               chunksize=chunksize,
                              # nrows=100000
                              )

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

rownum=[]
for c,x in enumerate(classification):
    header = (c==0)
    x = x.loc[x['SIC'].isin(sicset)]
    rownum.append(len(x))
    x.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classification_input_SASYYYYMMDD.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"subset file n = {sum(rownum)}")

#%% DATA CHECK

# how many records are there?:: 78,155,083
classification = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\classification_input_SASYYYYMMDD.txt', sep='\t', header=0, usecols=['DunsYear'], chunksize=100000000)
                             
lenlist = []

for x in classification:
    lenlist.append(len(x))
    print(len(x))      


print(sum(lenlist))
