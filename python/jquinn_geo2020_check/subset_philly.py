# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 15:14:39 2022

@author: stf45
"""

#%%
import pandas as pd
import numpy as np
from datetime import datetime
import time

#%% START TIMER

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

#%% READ IN JQUINN (NETS2020) DATASET (TWO FILES)

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', 
                      usecols={'behid', 'accu', 'uhc_x', 'uhc_y'}, 
                      header=0)
jquinn2 = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv',
                      usecols={'behid', 'loc_fyear', 'loc_lyear'}, 
                      header=0)

#%% MERGE JQUINN VARIABLES

# merge jquinn2 vars with jquinn
jquinn = jquinn.merge(jquinn2, left_on='behid', right_on='behid')

# add dunsnumber column (grabbed from last 9 digits of behid)
jquinn['DunsNumber'] = jquinn['behid'].astype(str).str[2:]

# delete jquinn2 to free space
del jquinn2

#%% READ IN DWALLS (NETS2019) DATASET
dwalls = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2xy.txt", sep='\t', 
                     usecols={'DunsMove', 'DunsNumber', 'GcFirstYear', 'GcLastYear'}, 
                     dtype={'DunsNumber': str},
                     header=0,
                     # nrows=1000,
                     )

#%% SUBSET FOR PHILLY, ADD DUNSNUMBER COLUMN TO PHILA_QUINN

# subset jquinn for XYs within phila bounding coords: (-75.280266,39.867004) 	(-74.955763,40.137992)
phila_quinn = jquinn.loc[(jquinn['uhc_x'] >= -75.280266) & (jquinn['uhc_x'] <= -74.955763) & (jquinn['uhc_y'] >= 39.867004) & (jquinn['uhc_y'] <= 40.137992)]

# subset dwalls for behids (aka 'DunsMove') matching those of jquinn
phila_dwalls = dwalls[dwalls['DunsMove'].isin(phila_quinn['behid'])]

# add dunsnumber column to phila_quinn
phila_quinn['DunsNumber'] = phila_quinn['behid'].astype(str).str[2:]

# check for any nulls in df
phila_quinn.isnull().values.any()