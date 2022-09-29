# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 10:45:39 2022

@author: stf45
"""

#%%
import pandas as pd
import numpy as np

#%% LOAD IN FILE FROM JAMES

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', usecols={'behid', 'uhc_x', 'uhc_y'}, header=0)

dwalls = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2xy.txt", sep='\t', usecols={'DunsMove', 'Latitude', 'Longitude'}, header=0)

#%%

x1 = jquinn['uhc_x']
x2 = dwalls['Longitude']

y1 = jquinn['uhc_y']
y2 = dwalls['Latitude']

compare = jquinn.merge(dwalls, left_on='behid', right_on='DunsMove')


