# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 15:05:26 2022

@author: stf45

This file takes the NETS 2020 geocoding csv file (second half, file #2) recieved from James Quinn on 
09/13/2022 and adds two new columns (uhc_x & uhc_y) prioritizing the "display" 
(rooftop or parcel centriod; dxwgs84, dywgs84) coordinates, as opposed to the "primary" 
(street-side/routing location; xwgs84 & ywgs84) coordinates. 

Input: \\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2020\NETS2020Geocoding\nets_tall_v1_20220913_2.csv
Output: \\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2020\NETS2020Geocoding\nets_tall_priority_xy20220916.csv

Runtime: approx 10 minutes

"""

#%%
import pandas as pd
import numpy as np

#%% LOAD IN FILE FROM JAMES
df = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_v1_20220913_2.csv', sep='|', dtype={'duns':str, 'behsic':str, 'myear':'Int64'}, header=0)

#%% SORT, REFORMAT

df.sort_values(by=['duns', 'behloc'], inplace=True)

# add new columns
df['uhc_x'] = np.where(df['dxwgs84'].isnull(), df['xwgs84'], df['dxwgs84'])
df['uhc_y'] = np.where(df['dywgs84'].isnull(), df['ywgs84'], df['dywgs84'])

#%% DUMP TO CSV

# dump file to csv, including all old columns 
df.to_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', index=False)
