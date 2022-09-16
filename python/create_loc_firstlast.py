# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 15:42:07 2022

@author: stf45

This file takes the NETS 2020 geocoding csv (first half, file #1) file recieved from James Quinn on 
09/13/2022 and adds two new columns referring to the first year and last year a
record exists at a particular location. Columns are: loc_fyear (first year at location) 
and loc_lyear (last year at location), where the year that a company moved is its last year 
in that location. First year in new location is move year + 1. Move data derived from 
NETS2020_Move['MoveYear'].

loc_fyear == myear + 1, unless myear is null, in which case loc_fyear == fyear
loc_lyear == myear lagged by 1, unless myear lag 1 is null, in which case loc_lyear == lyear

Input: \\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2020\NETS2020Geocoding\nets_tall_v1_20220913_1.csv
Output: \\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2020\NETS2020Geocoding\nets_tall_locfirstlast20220916.csv

Runtime: approx 6 minutes
"""
#%%

import pandas as pd
import numpy as np

#%% LOAD IN FILE FROM JAMES
df = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_v1_20220913_1.csv', sep='|', dtype={'duns':str, 'behsic':str, 'myear':'Int64'}, header=0)

#%% SORT, REFORMAT

df.sort_values(by=['duns', 'behloc'], inplace=True)

# add new columns
df['loc_fyear'] = np.where(df['myear'].isnull(), df['fyear'], df['myear']+1).astype(int)
df['loc_lyear'] = np.where(df['myear'].shift(1).isnull(), df['lyear'], df['myear'].shift(1))

#%% DUMP TO CSV

# dump file to csv, including all old columns 
df.to_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv', index=False)
