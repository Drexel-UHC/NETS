# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 14:45:53 2022

@author: stf45
"""


#%%
import pandas as pd
import numpy as np
from datetime import datetime
import time


#%% READ IN JQUINN (NETS2020) DATASET (TWO FILES)

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', 
                      usecols={'behid', 'uhc_x', 'uhc_y'}, 
                      header=0)
jquinn2 = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv',
                      usecols={'behid', 'behsic', 'loc_fyear', 'loc_lyear'}, 
                      header=0)

#%% MERGE JQUINN VARIABLES

# merge jquinn2 vars with jquinn
jquinn = jquinn.merge(jquinn2, left_on='behid', right_on='behid')

# delete jquinn2 to free space
del jquinn2

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

phila_quinn['sic_2'] = phila_quinn['behsic'].astype(str).str[:2]
phila_quinn.drop(columns=['DunsNumber','loc_fyear','loc_lyear', 'behid2020'],inplace=True)
phila_quinn.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_phila_poster.csv', index=False)



