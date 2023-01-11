# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 13:18:35 2022

@author: stf45
"""

import pandas as pd

df = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20221209\input\sic_cat_check20221209.csv', dtype={'SICCode':str})

subset = df.loc[(df['SIC Only Auxilary Code 1']!='X') & (df['In RECVD']!='0')]

subset.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20221209\nets_catcheck20221212.csv',index=False)
