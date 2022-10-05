# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 13:13:06 2022

@author: stf45

INPUTS:
dwalls = NETS 2019 data with (unique id is dunsnumber + move number: 'DunsMove')
jquinn = NETS 2020 data geocoded by jquinn (unique id is dunsnumber + move number: 'behid2020')

STATS:
jquinn (input)                                                       n=84,889,235
jquinn2019 (output; after all records loc_fyear==2020 removed)       n=79,456,025
difference b/t above (n records where loc_fyear==2020)               n= 5,433,210
n additional records in jquinn where behid is shifted due to moves in 2020      n=   313,881

dwalls (input)                                                       n=78,466,868
n records in jquinn whose behid2019 do not match any dwalls.DunsMove n= 1,506,900
n records in dwalls whose DunsMove do not match any jquinn.behid2019 n=   517,743
difference b/t dwalls and jquinn2019                                 n=   989,157
n records in dwalls whose dunsnumber do not match any in jquinn      n=    45,289

I think that 989,157 records (all pre-2020 data) were retroactively added to the 
NETS2020 dataset. These locations will not have matching business categorization data
and should be removed. It seems like there are 517,743 records in dwalls that aren't 
in jquinn and 517,743 that are in jquinn but not in dwalls after accounting for the
989,157 difference between the two. What does this mean? It appears that only 45,055
DunsNumbers appear in dwalls but not in jquinn2019, which only have an additional 234
Locations.

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
jquinn = jquinn.merge(jquinn2, left_on='behid', right_on='behid')

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

# rename behid behid2019, as these should now match DunsMove from NETS2019
jquinn.rename(columns={'behid': 'behid2019'}, inplace=True)

# duplicate behid column and make it called behid2020
jquinn['behid2020'] = jquinn['behid2019']

# if Move2020 == 1, behid = behid - 1000000000 (second digit aka move number -1)
jquinn.loc[jquinn['Move2020'] == 1, 'behid2019'] -= 1000000000

# make new df containing only records that share the same dunsnumber as a record where Move2020 ==1. 
shifted = jquinn.loc[jquinn['Move2020']==1, ['DunsNumber','behid2019','behid2020']]

# remove any record with loc_fyear 2020
jquinn = jquinn.loc[jquinn['loc_fyear']!=2020]

jquinn.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/jquinn2019.csv", index=False)

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

# from this subset^^, has the second digit been moved up +1 for all values in behid2019?:: Yes
shifted.loc[shifted['behid2019'] + 1000000000 != shifted['behid2020']].shape[0] == 0

#%% DATA CHECK 2

dwalls = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2xy.txt", sep='\t', 
                     usecols={'DunsMove', 'DunsNumber', 'GcLastYear'}, 
                     dtype={'DunsNumber': str},  
                     header=0,
                     # nrows=1000,
                     )

# how many records from the 2020 data do not match records from the 2019 data on behid2019 and DunsMove?: 
jquinn_notin_dwalls = jquinn.loc[~jquinn['behid2019'].isin(dwalls['DunsMove'])] # n = 1506900

# how many records from the 2020 data do not match records from the 2019 data on DunsNumbers?: 
jquinn_notin_dwalls_dunsonly = jquinn.loc[~jquinn['DunsNumber'].isin(dwalls['DunsNumber'])] # n = 1496087

# how many records from the 2019 data do not match records from the 2020 data on DunsMove and behid2019?:
dwalls_notin_jquinn = dwalls.loc[~dwalls['DunsMove'].isin(jquinn['behid2019'])] # n = 517743

# how many records from the 2019 data do not match records from the 2020 data on DunsNumbers?:
dwalls_notin_jquinn_dunsonly = dwalls.loc[~dwalls['DunsNumber'].isin(jquinn['DunsNumber'])] # n = 45289

dwalls_notin_jquinn_dunsonly['DunsNumber'].nunique()

print(jquinn.shape[0] - dwalls.shape[0]) # = 989157

#%% DATA CHECK 3:: RESOLVED


# the results below show that behid and Dunsmove have different results when
#the n of moves per dunsnumber is greater than 9. behid changes first digit to 2,
#while DunsMove adds extra digit. Must change dunsmove formula in geocoding_xy_compare_1.py
jquinn['behid2019'].max() # = 21037380375
dwalls['DunsMove'].max() # = 111037380375

#%% DATA CHECK 4 

move = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Move.txt', sep='\t', dtype= {'DunsNumber':str, 'MoveYear':float}, usecols=['DunsNumber', 'MoveYear'], encoding_errors='replace')

movematch = jquinn.merge(move, left_on=['DunsNumber', 'myear'], right_on=['DunsNumber', 'MoveYear'])

jquinn_duns = jquinn.groupby("DunsNumber")['behid2020']
jquinn_duns.rename(columns={'behid':'behid_count'}, inplace=True)
dwalls_duns = dwalls.groupby("DunsNumber")['DunsMove'].count().reset_index()
dwalls_duns.rename(columns={'DunsMove':'DunsMove_count'}, inplace=True)

# how many dunsnumbers from the 2020 data do not match dunsnumbers from 2019 data?: 6,678,441
# this is how many dunsnumbers were added to the 2020 dataset
dunsjd = jquinn.loc[jquinn['DunsNumber'].isin(dwalls['DunsNumber'])] 
print(shape(dunsjd))
dunsjd = dunsjd.loc[jquinn['Move2020'] = 1]

# create subset of jquinn excluding dunsnumbers that dont match dwalls (aka excluding new duns): 77,959,938
jquinn_oldduns = jquinn.loc[~jquinn['DunsNumber'].isin(dunsjd['DunsNumber'])]

# now add moves. how many additional locations is this?: 6,687,653 (6678441 + 9212 additional moves )
dunsjd['behid_count'].values.sum()

# how many records 
dunsjd_isin = jquinn_duns.loc[jquinn_duns['DunsNumber'].isin(dwalls_duns['DunsNumber'])] 

# how many dunsnumbers from the 2019 data are not in the 2020 data?: 45,055
# this is how many DunsNumbers were removed from the 2020 dataset
dunsdj = dwalls_duns.loc[~dwalls_duns['DunsNumber'].isin(jquinn_duns['DunsNumber'])] 

jbehid = jquinn[['behid2019', 'DunsNumber']]
ddunsmove = dwalls[['DunsMove', 'DunsNumber']]

match = jbehid.merge(ddunsmove, left_on='behid2019', right_on='DunsMove')
