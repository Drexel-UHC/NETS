# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This is a working draft file used to figure out how to link geocoding data with 
categorized (classify.py output) data.
"""

import pandas as pd

geocoding_2_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocoding_2_sample.txt', sep = '\t', dtype={'DunsNumber':str, 'GcZIP':str, 'GcZIP4':str},  header=0)
classification_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt', sep = '\t', dtype={'DunsNumber':str},  header=0)


new_df = geocoding_2_sample.merge(classification_sample, on='DunsNumber', how='left')
df2 = new_df.loc[(new_df['YearFull'] >= new_df['GcFirstYear']) & (new_df['YearFull'] <= new_df['GcLastYear'])]



        
        
        
        
        
    



