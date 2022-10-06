# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 14:12:16 2022

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

#%% DATA CHECK 1: how many records are new businesses that started in 2020?:: 5,191,566

new2020 = jquinn.loc[(jquinn['loc_fyear']==2020) & (~jquinn['DunsNumber'].isin(dwalls['DunsNumber']))].shape[0]

#%% DATA CHECK 3: how many records are new locations of an existing business whose first year is 2020?:: 241,644
old2020 = jquinn.loc[(jquinn['loc_fyear']==2020) & (jquinn['DunsNumber'].isin(dwalls['DunsNumber']))].shape[0]

# how many total records have their first year as 2020?:: 5,433,210
new2020 + old2020

#%% DATA CHECK 4: how many records from 2020 dataset match with 2019 dataset on dunsmove?:: 77,949,125
    
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

# how many records from 2020 dataset match with 2019 dataset on dunsnumber + dunsmove?
# merge jquinn and dwalls datasets, keeping first year last year at location to check match later
merged = jquinn[['DunsNumber', 'behid2019', 'loc_fyear', 'loc_lyear']].merge(dwalls, left_on='behid2019', right_on='DunsMove')
merged.shape[0]

#%% DATACHECK 5: how many records from 2020 dataset match with 2019 dataset on dunsnumber 
# + first year at location + last year at location?:: 77,157,372
    
merged.loc[merged['loc_lyear']==2020, 'loc_lyear'] = 2019
perfect_match = merged.loc[(merged['loc_fyear'] == merged['GcFirstYear']) & (merged['loc_lyear'] == merged['GcLastYear'])]
perfect_match.shape[0]


#%% DATACHECK 2: how many pre-2020 records from 2020 dataset have DunsNumbers that are not in 2019 dataset?::1,496,087

# Records in 2020 dataset with DunsNumbers that are not in 2019 dataset, But First Year < 2020

retroadd = jquinn.loc[(jquinn['loc_fyear']<2020) & (~jquinn['DunsNumber'].isin(dwalls['DunsNumber']))]

retro_moveonly = jquinn.loc[(jquinn['loc_fyear']<2020) & (~jquinn['behid2019'].isin(dwalls['DunsMove']))]

#%% DATACHECK 6: A. how many records from the 2019 data do not match records from the 2020 data on DunsNumbers? (how many dunsmoves were removed from 2020):: 45,289
               # B. how many DunsNumbers from the 2019 data are not in the 2020 data? (how many dunsnumbers were removed from 2020):: 45,055
    
# dunsmoves removed from 2020 dataset: 45,055 
dwalls.loc[~dwalls['DunsNumber'].isin(jquinn['DunsNumber'])].shape[0] # n = 45289

# dunsnumbers removed from 2020 dataset: 45,055
dunsnums_rm = dwalls.loc[~dwalls['DunsNumber'].isin(jquinn['DunsNumber'])]
dunsnums_rm.drop_duplicates(subset=['DunsNumber']).shape[0]

#%% END TIMER

toc = time.perf_counter()
t = toc - (sum(time_list) + tic)
time_list.append(t)
runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f"End Time: {datetime.now()}")
