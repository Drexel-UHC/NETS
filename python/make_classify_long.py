# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 14:33:42 2022

@author: stf45

This is a script to convert the Classified Dataset (Python) (classifiedYYYYMMDD.txt) from wide to long. 

runtime: ~125 mins
"""
#%% 
import pandas as pd
import time
from datetime import datetime
    
#%% READ CLASSIFIED DATASET (PYTHON) FILE

n = 303004623
chunksize = 5000000
coded_reader = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classified_python20231024.txt', sep = '\t', encoding_errors='replace', header=0, 
                               chunksize=chunksize)

#%% CREATE CATEGORY/CODE TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

output_rownum = []
for c, x in enumerate(coded_reader):
    header = (c==0)
    x = pd.melt(x, id_vars=['DunsYear'], var_name='BaseGroup')
    x = x.loc[x['value']==1]
    x = x.drop(columns=['value'])
    x.to_csv(r"D:/scratch/classified_longYYYYMMDD.txt", sep="\t", header=header, mode='a', index=False)
    output_rownum.append(len(x))
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

print(f'output n = {sum(output_rownum)}') # 310,942,541
