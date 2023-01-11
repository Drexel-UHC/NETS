# -*- coding: utf-8 -*-
"""
Created on Wed Dec 21 09:45:50 2022

@author: stf45


YOU DONT NEED THIS. CHECK DAILY NOTES 1/04/2023 FOR MEETING WITH MATT.

"""

# %%

import pandas as pd
import time
import json
from datetime import datetime
import os

#%% LOAD JSON CONFIG

with open(r'C:\Users\stf45\Documents\NETS\Processing\config/main_cat_config20221221.json', 'r') as f:
    main_config = json.load(f)
    
#%%

def classify_main(classified_aux, main_config, header):
    maincats = []
    for maincat in main_config.keys():
        maincat_bool = x["variable"].isin(main_config[maincat]['auxiliary'])
        maincats.append(pd.DataFrame({maincat: maincat_bool*1}))
    out_main = pd.concat(maincats,axis=1)
    final_main = pd.concat([x, out_main],axis=1)
    final_main.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_main_sample.txt", sep="\t", header=header, mode='a', index=False)

#%% READ FILE

n = 2679
chunksize = 500
coded_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_long_sample.txt', sep='\t', header=0, 
                              chunksize=chunksize)

#%% CREATE CATEGORY/CODE TABLE

# cat_table = pd.DataFrame({'cat_name':config.keys(), 'cat_id': pd.Series(range(len(config)))})

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, x in enumerate(coded_reader):
    header = (c==0)
    classify_main(x, main_config, header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% DATA CHECK

check = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_main_sample.txt", sep="\t")

dups = check.loc[check.duplicated(subset=['DunsYear'], keep=False)]
#%% remove

os.remove(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_main_sample.txt")


