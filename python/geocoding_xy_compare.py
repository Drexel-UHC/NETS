# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 10:06:02 2022

@author: stf45
"""

#%%
import pandas as pd
import numpy as np

#%% LOAD IN FILE FROM JAMES

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', usecols={'behsic', 'uhc_x', 'uhc_y'}, header=0)

dwalls = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_locfirstlast20220916.csv', usecols={'behsic', 'uhc_x', 'uhc_y'}, header=0)

