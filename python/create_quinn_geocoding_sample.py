# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 15:46:14 2022

@author: stf45
"""

#%%
import pandas as pd
from random import sample

#%% LOAD IN GEOCODING FILE 1

df = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_v1_20220913_1.csv', sep = '|', dtype=object, header=0,
                                     usecols=['behid',
                                              'duns',
                                              'behloc',
                                              'fyear',
                                              'lyear',
                                              'myear'])

priority_xy = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', dtype=object, header=0)

first_last = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv', sep = '|', dtype=object, header=0)

#%% CREATE RANDOM SAMPLE OF NETS TALL V1 N=1000

sample = df['duns'].sample(1000)
sample = pd.DataFrame(sample)

merge = sample.merge(df, on='duns')

merge.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\nets_tall_v1_20220913_1_sample.csv', index=False)

#%% CREATE RANDOM SAMPLE OF PRIORITY XY N=1000

merge2 = sample.merge(priority_xy, on='duns')

merge2.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\nets_tall_priority_xy20220916_sample.csv', index=False)

#%% CREATE RANDOM SAMPLE OF FIRST LAST N=1000


merge3 = sample.merge(first_last, on='duns')

merge3.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\nets_tall_locfirstlast20220916.csv', index=False)
