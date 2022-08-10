# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 08:44:48 2022

@author: stf45
"""

import pandas as pd
import time
import json
import nets_functions as nf
import warnings 
from itertools import product

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

#%% LOAD JSON CONFIG

with open(r'C:\Users\stf45\Documents\NETS/processing/config/json_config_2022_04_20_MAR.json', 'r') as f:
    config = json.load(f)
    
    
#%% FUNCTIONS FOR CREATING SIC LISTS FROM SIC RANGES IN CONFIG

def make_sic_range(cat,config):
    sic_range = []
    for i in zip(config[cat]["sic_range"][0::2], config[cat]["sic_range"][1::2]):
        sic_range.extend(list(range(i[0], i[1]+1)))
    return sic_range

def make_sic_range2(cat,config):
    sic_range_2 = []
    for i in zip(config[cat]["sic_range_2"][0::2], config[cat]["sic_range_2"][1::2]):
        sic_range_2.extend(list(range(i[0], i[1]+1)))
    return sic_range_2

    
#%% GETSICS FUNCTION

# this function takes in the config and and category (cat) as inputs and grabs 
#SICs from any sic_range and sic_exclusive lists from the cat in the config and 
#dumps it into a single list. ranges are expanded into full lists of every integer
#within the range (inclusive).

def getsics(config, cat):
    try:
        sic_range = make_sic_range(cat,config)
    except KeyError:
        sic_range = []
    try:
        sic_exclusive = config[cat]["sic_exclusive"]
    except KeyError:
        sic_exclusive = []
    
    try:
        sic_range2 = make_sic_range2(cat,config)
    except KeyError:
        sic_range2 = []
    try:
        sic_exclusive2 = config[cat]["sic_exclusive_2"]
    except KeyError:
        sic_exclusive2 = []
    siclist = sic_range + sic_exclusive + sic_range2 + sic_exclusive2
    return siclist
    
#%% CHECK EACH CATEGORY'S SICS AGAINST THE OTHERS

# iterate through the config, check sics from each cat against every other cat.
#skip check if both cats are the same. dump any matching sics into a list (matchlist)

# matchlist = []
# keys = config.keys()

# for cat in product(enumerate(keys),enumerate(keys)):
#     if cat[0][0] >= cat[1][0]:
#         continue 
#     sicvar = getsics(config,cat[0][1])
#     sicvar1 = getsics(config,cat[1][1])  
#     matches = set(sicvar).intersection(set(sicvar1))
#     if len(matches) > 0:
#         matchlist.append((cat[0][1], cat[1][1]))

    
#%% CHECK ONLY OWR SICS AGAINST ALL OTHERS

matchlist = []
keys = config.keys()


for cat in config.keys():
    print(cat)
    if cat == 'OWR':
        continue
    owrsics = getsics(config,'OWR')
    sicvar1 = getsics(config,cat)  
    matches = set(owrsics).intersection(set(sicvar1))
    if len(matches) > 0:
        matchlist.append((cat, matches))
    
#%% CREATE DATAFRAME FROM MATCHLIST

matchdf = pd.DataFrame(matchlist)

#%% WRITE EXCEL REPORT

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS/processing/reports/SIC_overlap_report.xlsx') as writer:
    matchdf.to_excel(writer, sheet_name='unique_values', index=False)    
    

    
    
    
    
    
    
    
    
    
