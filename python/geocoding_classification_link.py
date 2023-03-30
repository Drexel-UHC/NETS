# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This file creates the linkage between DunsYear and DunsMove by merging the classification input file and the 
geocoding input file on dunsnumber, and subsetting for where DunsMove year ranges match DunsYears.
"""

#%%
import pandas as pd

geocoding_2_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocoding_2_sample.txt', sep = '\t', dtype={'DunsNumber':str}, usecols = ['DunsNumber','DunsMove','GcFirstYear','GcLastYear'], header=0)
classification_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt', sep = '\t', dtype={'DunsNumber':str}, usecols = ['DunsNumber','DunsYear','YearFull'],  header=0)

new_df = geocoding_2_sample.merge(classification_sample, on='DunsNumber', how='left')
df2 = new_df.loc[(new_df['YearFull'] >= new_df['GcFirstYear']) & (new_df['YearFull'] <= new_df['GcLastYear'])]
df23 = new_df.loc[~(new_df['YearFull'] >= new_df['GcFirstYear']) & (new_df['YearFull'] <= new_df['GcLastYear'])]

df2 = df2[['DunsYear','DunsMove']]

df2.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\DunsMove_DunsYear_Key.txt', sep='\t', index=False)
        
        
        
        
        
    



