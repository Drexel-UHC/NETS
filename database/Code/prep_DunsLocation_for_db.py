# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 14:34:43 2023

@author: stf45
"""

import pandas as pd

#%%

df = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\DunsLocation20230628.csv', dtype={'GEOID10':str})

df = df.rename(columns={'OID_':'DunsLocationId','USER_AddressID':'AddressID', 'POINT_X':'Xcoord', 'POINT_Y':'Ycoord', 'area10km':'AreaLand', 'ALAND10km':'TotalArea'})

df = df[['DunsLocationId', 
         'AddressID', 
         'Xcoord', 
         'Ycoord',
         'DisplayX', 
         'DisplayY',
         'GEOID10',  
         'AreaLand', 
         'TotalArea', 
         'Addr_type', 
         'Status',
         'Score', 
         'UHCMatchCodeRank']]

df.to_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\DunsLocationDB20230823.csv', index=False)


