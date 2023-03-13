# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 14:15:54 2023

@author: stf45
"""

import pandas as pd

df = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20230203\input\sic_cat_check20230203.csv', dtype={'SICCode':str})

subset = df.loc[(df['SIC Only Auxilary Code 1'] == 'X') & (df['In RECVD'].isin(['1','3']))]

# results: 0 records. 
