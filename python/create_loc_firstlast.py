# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 15:42:07 2022

@author: stf45
"""




import pandas as pd
import numpy as np

df = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocode_sample20220915.csv', dtype={'duns':str}, header=0)

#%%

df['loc_fyear'] = np.where(df['myear'].isnull(), df['fyear'], df['myear']+1)
df['loc_lyear'] = np.where(df['myear'].shift(1).isnull(), df['lyear'], df['myear'].shift(1))
