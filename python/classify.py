# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This script is used to categorized establishments into health-related Base Group categories (n=176). 
"""

#%%
import os
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import pandas as pd
import time
import json
import nets_functions as nf
import warnings 
from datetime import datetime

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

#%% READ IN FILES

# load in json config. this has all aux categories and their conditions.
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230329.json', 'r') as f:
    config = json.load(f)

# FULL FILE
chunksize = 5000000
n = 353116375
file = r'C:\Users\stf45\Documents\NETS\Processing\scratch\classify_samples\classification_input_PythonYYYYMMDD.txt'

# SAMPLE FILE
# chunksize = 1000
# n = 7681
# file = r'C:\Users\stf45\Documents\NETS\Processing\scratch\classify_samples\classification.txt'


class_long_reader = pd.read_csv(file, sep='\t', dtype={'DunsNumber':str, 'SIC':int, 'Emp':int, 'Sales':int}, 
                                chunksize=chunksize,
                                header=0,
                                # skiprows=range(1, 1001)
                                )

#%% CLASSIFY


print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, class_chunk in enumerate(class_long_reader):
    header = (c==0)
    nf.classify(class_chunk, config, header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))    


runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% GET FREQS 

# get sum of each record's total category count
# get unique values to show how many records were not flagged (0), flagged once (1), etc
classified['cat_counts'] = classified.iloc[:,8:].sum(axis=1)
uniques = classified['cat_counts'].value_counts()
uniques = pd.DataFrame(uniques)


# get sum of each category's total record count
catcounts = classified.iloc[:,8:].sum(axis=0)
catcounts = pd.DataFrame(catcounts, columns=['count'])

#%% WRITE EXCEL REPORT

with pd.ExcelWriter('../reports/NETS_classify_report20220509.xlsx') as writer:
    uniques.to_excel(writer, sheet_name='unique_values')
    catcounts.to_excel(writer, sheet_name='cat_counts')    
