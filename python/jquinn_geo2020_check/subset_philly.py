# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 15:14:39 2022

@author: stf45

Runtime: approx 6 mins
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
                     usecols={'DunsMove', 'DunsNumber', 'GcFirstYear', 'GcLastYear', 'Longitude', 'Latitude'}, 
                     dtype={'DunsNumber': str},
                     header=0,
                     # nrows=1000,
                     )

#%% REFORMAT BEHID
    

# Move Num -1 if first year == 2020 for all records with same dunsnumber

# create list of dunsnumbers where loc_fyear == 2020
dunslist = jquinn.loc[jquinn['loc_fyear']==2020, 'DunsNumber'].tolist()

# if record has dunsnumber in dunslist, new column "Move2020" == 1. All others nan
jquinn.loc[jquinn['DunsNumber'].isin(dunslist), "Move2020"] = 1 

# rename behid behid2019, as these should now match DunsMove from NETS2019
jquinn.rename(columns={'behid': 'behid2019'}, inplace=True)

# duplicate behid column and make it called behid2020
jquinn['behid2020'] = jquinn['behid2019']

# if Move2020 == 1, behid = behid - 1000000000 (second digit aka move number -1)
jquinn.loc[jquinn['Move2020'] == 1, 'behid2019'] -= 1000000000

#%% SUBSET PHILLY, OUTPUT DFS

# subset jquinn for XYs within phila bounding coords: (-75.280266,39.867004) 	(-74.955763,40.137992)
phila_quinn = jquinn.loc[(jquinn['uhc_x'] >= -75.280266) & (jquinn['uhc_x'] <= -74.955763) & (jquinn['uhc_y'] >= 39.867004) & (jquinn['uhc_y'] <= 40.137992)]

# subset for business locations whose first year is before 2020
phila_quinn = phila_quinn.loc[phila_quinn['loc_fyear']<2020]

# convert last year == 2020 to last year == 2019
phila_quinn.loc[phila_quinn['loc_lyear']==2020, 'loc_lyear'] = 2019

# change move2020 column nans to zeros, make int dtype
phila_quinn.loc[phila_quinn['Move2020'].isnull(), 'Move2020']=0
phila_quinn['Move2020'] = phila_quinn['Move2020'].astype(int)

# subset dwalls for behids (aka 'DunsMove') matching those of jquinn
phila_dwalls = dwalls[dwalls['DunsMove'].isin(phila_quinn['behid2019'])] 

# check for any nulls in phila_quinn other than move2020 col
phila_quinn.isnull().values.any()

# check for any nulls in phila_dwalls
phila_dwalls.isnull().values.any()

phila_quinn = phila_quinn.loc[phila_quinn['behid2019'].isin(phila_dwalls['DunsMove'])]

# check how many records have non-matching first year values
fyear_mismatch = merged.loc[(merged['GcFirstYear'] != merged['loc_fyear'])]


phila_quinn.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_phila_quinn.csv', index=False)
phila_dwalls.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_phila_dwalls.csv', index=False)

#%% END TIMER

toc = time.perf_counter()
t = toc - (sum(time_list) + tic)
time_list.append(t)
runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)                                                                                                
print(f"End Time: {datetime.now()}")
