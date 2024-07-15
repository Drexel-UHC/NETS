# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 10:08:28 2024

@author: stf45
"""

import pandas as pd

df = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\Tract_ZCTA_Hierarchy\BEDDN_t10_measures_hier20240506.txt', sep='\t')

#%%
byyear = (df.groupby('Year')
          .sum()
          .drop(columns='tract10')
          )

#%% GET DICT OF YEARS FOR WHICH THERE ARE 0 HITS FOR EACH CATEGORY

results = {'cat':[], 'year':[]}
for col in byyear.columns:
    zeros = byyear.loc[byyear[col]==0]
    years = list(zeros.index)
    results['cat'].append(col)
    results['year'].append(years)
 

check = pd.DataFrame(results)

#%% 

byyear.to_csv(r'D:\scratch\beddn_t10_yearly_sums.csv')
