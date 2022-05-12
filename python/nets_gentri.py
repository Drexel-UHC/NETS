# -*- coding: utf-8 -*-
"""
Created on Thu May 12 09:27:21 2022

@author: stf45
"""

import pandas as pd


#%%

sic_check = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\data_checks\sic_check.txt", sep='\t', dtype={"SIC19":str, "DunsNumber":str},)

sic_desc = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\sic_potential_adds.txt', sep = '\t', dtype={"SICCode":str},  header=0, encoding_errors='replace', usecols=['SICCode', 'SICDescription', 'Jana Comment/potential grouping'])

df = pd.merge(sic_check, sic_desc, left_on='SIC19', right_on='SICCode')

sic_types = ['Arts/culture', 'Construction for development/gentrification', 'For development? Or redlining stuff?', 'Maybe for renovations?']
df = df[df['Jana Comment/potential grouping'].isin(sic_types)].drop(columns=['SICCode'])
df.SIC19.str.zfill(8)
df.Longitude = df.Longitude*-1

df.to_csv(r"C:\Users\stf45\Arc_Projects\NETS\NETS_SIC_check\data\nets_gentri.txt")
