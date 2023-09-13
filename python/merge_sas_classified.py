# -*- coding: utf-8 -*-
"""
Created on Thu May 25 11:48:56 2023

@author: stf45
"""

import pandas as pd

#%%

df1 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\cmassmerch.csv')
df2 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\conv_gas.csv')
df3 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\conv_other.csv')
df4 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\drug.csv')
df5 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\grocery.csv')
df6 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\supercenter.csv')
df7 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\gmassmerch.csv')
df8 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\warehouse.csv')

#%% MERGE INDIVIDUAL SAS CLASSIFIED DATASETS

dflist = [df1,df2,df3,df4,df5,df6,df7,df8]
lenlist=[]
dfmain = pd.DataFrame(columns=['DunsYear'])
for df in dflist:
    lenlist.append(len(df))
    dfmain = dfmain.merge(df, how="outer")
    
    print(f'length of main: {len(dfmain)}')
    print(f'length of total up to {df.columns[1]}: {sum(lenlist)}')

# replace nans with zeros and switch to int dtype
dfmain = dfmain.fillna(0)
dfmain.iloc[:,1:] = dfmain.iloc[:,1:].astype(int)

#%% DATA CHECK AND EXPORT TO CSV

# find # of DunsYears that are flagged more than once
dups = dfmain.loc[dfmain.sum(axis=1,numeric_only=True) > 1]

#check to see if any DunsYears are flagged more than twice
len(dfmain.loc[dfmain.sum(axis=1,numeric_only=True) > 2]) == 0

# see if total # of DunsYears in input is equal to output DunsYears, counting DunsYears
#twice that are flagged twice 
sum(lenlist) - (len(dups) + len(dfmain) )

dfmainhead = dfmain.head(500000)

dfmain.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_sas20230526.txt', sep='\t', index=False)


