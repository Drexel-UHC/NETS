# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 13:13:06 2022

@author: stf45

INPUTS:
dwalls = NETS 2019 data with (unique id is dunsnumber + move number: 'DunsMove')
jquinn = NETS 2020 data geocoded by jquinn (unique id is dunsnumber + move number: 'behid2020')


jquinn (input)                                                       n=84,889,235
jquinn2019 (after all records loc_fyear==2020 removed)               n=79,456,025
difference b/t above (n records where loc_fyear==2020)               n= 5,433,210
n records in jquinn where behid is shifted due to moves in 2020      n=   313,881

dwalls (input)                                                       n=78,466,868
n records where dwalls.dunsmove not in jquinn.behid2019              n= 1,506,907
n records where jquinn.behid2019 not in dwalls.dunsmove              n=   517,750
difference b/t dwalls and jquinn2019                                 n=   989,157
"""

#%%
import pandas as pd
import numpy as np
from datetime import datetime
import time

#%% READ IN CSVS

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', 
                      usecols={'behid', 'accu', 'uhc_x', 'uhc_y'}, 
                      header=0)
jquinn2 = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv',
                      usecols={'behid', 'loc_fyear', 'loc_lyear'}, 
                      header=0)

#%% MERGE JQUINN VARIABLES

# merge jquinn2 vars with jquinn
jquinn = jquinn.merge(jquinn2, on='behid')

# add dunsnumber column (grabbed from last 9 digits of behid)
jquinn['DunsNumber'] = jquinn['behid'].astype(str).str[2:]

# delete jquinn2 to free space
del jquinn2

#%% SHIFT BEHID 2ND DIGIT TO MATCH 2019 DUNSMOVE

# -1 IF FIRST YEAR = 2020 FOR ALL RECORDS WITH SAME DUNS  

# start runtime
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

# create list of dunsnumbers where loc_fyear == 2020
dunslist = jquinn.loc[jquinn['loc_fyear']==2020, 'DunsNumber'].tolist()

# if record has dunsnumber in dunslist, new column "Move2020" == 1. All others nan
jquinn.loc[jquinn['DunsNumber'].isin(dunslist), "Move2020"] = 1 

# remove any record with loc_fyear 2020
jquinn = jquinn.loc[jquinn['loc_fyear']!=2020]

# dupliecate behid column and make it called behid2020
jquinn['behid2020'] = jquinn['behid']

# if Move2020 == 1, behid = behid - 1000000000 (second digit aka move number -1)
jquinn.loc[jquinn['Move2020'] == 1, 'behid'] -= 1000000000

# rename behid behid2019, as these should now match DunsMove from NETS2019
jquinn.rename(columns={'behid': 'behid2019'}, inplace=True)

# end of runtime
toc = time.perf_counter()
t = toc - (sum(time_list) + tic)
time_list.append(t)
runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% DATA CHECK 

# how many records changed behid from above processing?

# create new df of both behid columns where Move2020==1
shifted = jquinn.loc[jquinn['Move2020']==1, ['behid2019','behid2020']]
# from this subset^^, has the second digit been moved up +1 for all values in behid2019? T/F
shifted.loc[diff['behid2019'] + 1000000000 != shifted['behid2020']].shape[0] == 0

#%% DATA CHECK 2

dwalls = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2xy.txt", sep='\t', usecols={'DunsMove', 'DunsNumber'}, dtype={'DunsNumber': str},  header=0)

# how many records from the 2020 data match that of the 2019 data on behid2019 and DunsMove?: 
print(jquinn.loc[~jquinn['behid2019'].isin(dwalls['DunsMove'])].shape[0]) # = 1506907

# how many records from the 20109 data match that of the 2020 data on DunsMove and behid2019?:
print(dwalls.loc[~dwalls['DunsMove'].isin(jquinn['behid2019'])].shape[0]) # = 517750

print(jquinn.shape[0] - dwalls.shape[0]) # = 989157

#%% DATA CHECK 3

dwalls.DunsNumber.value_counts().max()

# the results below show that behid and Dunsmove have different results when
#the n of moves per dunsnumber is greater than 9. behid changes first digit to 2,
#while DunsMove adds extra digit. Must change dunsmove formula in geocoding_xy_compare_1.py
jquinn['behid2019'].max() # = 21037380375
dwalls['DunsMove'].max() # = 111037380375



