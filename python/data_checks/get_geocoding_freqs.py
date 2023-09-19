# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 10:23:03 2023

@author: stf45
"""

import pandas as pd


tracts = pd.read_csv(r'C:\Users\stf45\Arc_Projects\NETS\output\NETS_locs_CT10AEAC_20230627.csv', dtype={'GEOID10':str})


tractcounts = tracts['GEOID10'].value_counts()
nanx = tracts.loc[tracts['Status'] == "U"]
nanaddr = tracts.loc[tracts['Addr_type'].isna()==True]
nantract = tracts.loc[tracts['GEOID10'].isna()==True]

closest = tracts.loc[tracts['ct10_distance'] > 0]
