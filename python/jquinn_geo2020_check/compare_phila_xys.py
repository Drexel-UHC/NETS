# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 10:40:15 2022

@author: stf45
"""

#%%
import pandas as pd
import numpy as np

#%% READ IN PROJECTED DATASETS

jquinn = pd.read_csv(r'C:\Users\stf45\Arc_Projects\NETS\output\phila_quinn_proj.csv', 
                      dtype={'DunsNumber':str},
                      header=0,
                      usecols={'behid2019','POINT_X','POINT_Y'})
dwalls = pd.read_csv(r'C:\Users\stf45\Arc_Projects\NETS\output\phila_dwalls_proj.csv',
                      dtype={'DunsNumber':str},
                      header=0,
                      usecols={'DunsMove','POINT_X','POINT_Y'})

#%%

jquinn.rename(columns={'POINT_X':'quinn_x','POINT_Y':'quinn_y'}, inplace=True)
dwalls.rename(columns={'POINT_X':'dwalls_x','POINT_Y':'dwalls_y'}, inplace=True)

compare = jquinn.merge(dwalls, left_on='behid2019', right_on='DunsMove')
compare.drop(columns=['DunsMove'], inplace=True)

#%%

compare['xy_distance_meters'] = np.sqrt((compare['quinn_x'] - compare['dwalls_x'])**2 + (compare['quinn_y'] - compare['dwalls_y'])**2)

stats = compare['xy_distance_meters'].describe().reset_index()
stats.rename(columns={'index':'metric','xy_distance_meters':'value'},inplace=True)

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\compare_xys20221019.xlsx') as writer:
    compare.to_excel(writer, "compare XY values", index=False)
    stats.to_excel(writer, "descriptives on compare_xy column", index=False)
    