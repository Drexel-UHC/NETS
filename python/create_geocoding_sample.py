# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 15:46:14 2022

@author: stf45
"""

#%%
import pandas as pd
from random import sample

#%% LOAD IN GEOCODING FILE 1

df = pd.read_csv(r'D:\NETS\NETS_2020\nets_tall_v1_20220913_1.csv', sep = '|', dtype=object, header=0,
                                     usecols=['behid',
                                              'duns',
                                              'behloc',
                                              'fyear',
                                              'lyear',
                                              'myear'],
                                   # chunksize=comp_chunksize,
                                   )


#%% CREATE RANDOM SAMPLE OF GEOCODING FILE 1 N=1000

sample = df['duns'].sample(1000)
sample = pd.DataFrame(sample)

merge = sample.merge(df, on='duns')

merge.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocode_sample20220915.csv')
