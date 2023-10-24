# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 15:47:08 2023

@author: stf45

This file takes in the classification input dataset (classification_input20230213.txt, n = 564,824,373) and removes records with SICs 
that are not used in MESA Neigh Aging NETS categorization. The output is a new subset
file (classification_input_PythonYYYYMMDD.txt) that will be used in the main NETS classification process (classify.py).


total time: approx 67 mins
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
    if config[cat]['conditional'] in [2,3,6,10,13,15]:
        sics = nf.make_sic_range(cat,config)
        sicset.update(sics)
    elif config[cat]['conditional'] in [7,12]:
        # sics = nf.make_sic_range2(cat,config)
        # sicset.update(sics)
        pass
    elif config[cat]['conditional'] in [4]:
        sics = nf.make_sic_ex_range(cat,config)
        sicset.update(sics)
    elif config[cat]['conditional'] in [8,9]:
        sics = config[cat]['sic_exclusive']
        sicset.update(sics)
    elif config[cat]['conditional'] in [11]:
        sics = config[cat]['sic_exclusive']
        sics2 = nf.make_sic_range2(cat,config)
        sicset.update(sics,sics2)
    elif config[cat]['conditional'] in [14]:
        print(f'{cat}: Processed in SAS')
    else:
        print(f'missing condit for {cat}')


#%% FILTER classification.txt FOR SICS IN SET

chunksize=75000000
n=694019684
classification = pd.read_csv(r'D:\scratch\classification_inputYYYYMMDD.txt', sep='\t', header=0, dtype={'DunsNumber':str},
                               chunksize=chunksize,
                              # nrows=100000
                              )

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

input_rownum = []
output_rownum = []
for c,x in enumerate(classification):
    header = (c==0)
    input_rownum.append(len(x))
    x = x.loc[x['SIC'].isin(sicset)]
    output_rownum.append(len(x))
    x.to_csv(r'D:\scratch\classification_input_PythonYYYYMMDD.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"input file n = {sum(input_rownum)}") #694,019,684
print(f"output file n = {sum(output_rownum)}") #435,928,419

