# -*- coding: utf-8 -*-
"""
Created on Fri Nov  4 08:58:24 2022

@author: stf45
"""

import pandas as pd
from datetime import datetime
import time

#%%
chunksize=10000000
n=78466868

geo_reader = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\geocoding_2.txt', sep='\t', chunksize=chunksize)

#%%

lens=[]

#START
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for i, c in enumerate(geo_reader):
    header = (i==0)
    lens.append(len(c))
    c.drop(columns=['DunsNumber', 'MoveNum', 'GcFirstYear', 'GcLastYear'], inplace=True)
    c.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\geocoding_2_paireddown2.txt', sep='|', index=False, mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(i+1, round(t/60, 2), n/chunksize-(i+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)    

print(sum(lens))