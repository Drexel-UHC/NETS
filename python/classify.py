# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

testing with chunksize 100,000, 1 chunk takes 1.54 mins, expected time for full (non-subset) file = 6.03 days.
we can't subset the input classification file because of CMU category (requires name search over all sics)'


testing chunksize of 100,000,000 overnight 02/13-02/14:: maxed out close to end.

testing chunksize of 85,000,000 overnight 2/14-2/15:: maxed out after ZOO classified. 
probably from concat

testing chunksize of 50,000,000 overnight 2/15-2/16:: maxed out after ZOO classified, from concat.

testing chunksize of 30,000,000 overnight 2/16-2/17

test skipping nrows to start at chunk 2 on sample:: THIS WORKS. just have to run:
    classified_nodups = classified.drop_duplicates() to drop any duplicates.
    make sure to uncomment skiprows, fill with the proper chunksize, and make header=False
    in CLASSIFY cell loop.
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
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230227.json', 'r') as f:
    config = json.load(f)

# FULL FILE
# chunksize = 5000000
# n = 564824373
# file = r'D:\NETS\NETS_2019\ProcessedData\classification_input20230213.txt'

# SAMPLE FILE
chunksize = 1000
n = 7681
file = r'C:\Users\stf45\Documents\NETS\Processing\scratch\classify_samples\classification.txt'


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

#%%
classified = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified.txt', sep='\t',  
                          nrows=30000
                         )

#%% DATA CHECK

# print(classification['DunsYear'].nunique()==classified['DunsYear'].nunique())

#%% WRITE OUT REPORT

title = "NETS Classification Report Using classify.py\n"

lines = [title, runtime]
with open('classify_report.txt', 'w') as f:
    for line in lines:
        f.write(line)
        f.write('\n')

#%% GET FREQS something is deprecated here, this needs to be rewritten

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
