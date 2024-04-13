# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 10:01:29 2024

@author: stf45

a sql-style script to pull records from the final BEDDN flat files.
"""

import pandas as pd
import os
from datetime import datetime
import time

print(f"Start Time: {datetime.now()}")
tic = time.perf_counter()

###############################################################################
#%% FILES

classifiedlong_file = r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\ClassifiedLong20231127.txt'
dunsmove_file = r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\DunsMove20231201.txt'
cat_descriptions_file = r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\CategoryDescriptions20231127.txt'
dunslocation_file = r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\DunsLocation20231207.txt'
xwalk_file = r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Final\BG_CC_TC_Xwalk20231023.txt'
output_folder = r'D:\scratch'

#%% USER INPUTS

# read in descriptions file, get list of domains
desc = pd.read_csv(cat_descriptions_file, sep='\t')
domlist = list(desc['Domain'].unique())
print(domlist)

# provide list of categories by entire domain
domains = ['Healthcare']

# provide list of individual categories
categories = ['DLR', 'CMN', 'GRY']

# provide list of year(s)
years = [2000, 2005, 2010, 2015]

# use hierarchy? True or False
hierarchy = True

###############################################################################

#%% SUBSET LIST OF CATEGORIES

# grab all categories in chosen domain(s)
domain_cats = desc['Category'].loc[desc['Domain'].isin(domains)]  
all_cats = list(domain_cats)

# grab all additional categories
[all_cats.append(category) for category in categories]

#%% READ IN CLASSIFIED LONG AND SUBSET BY CATEGORY

classlong = pd.read_csv(classifiedlong_file, sep='\t', usecols=['DunsYear','BaseGroup'])

# subset for provided categories
measures = classlong.loc[classlong['BaseGroup'].isin(all_cats)]
del classlong

#%% READ IN DUNSMOVE AND SUBSET BY YEAR

# merge in dunsmove columns
dunsmove = pd.read_csv(dunsmove_file, sep='\t', usecols=['DunsYear', 'DunsMove', 'AddressID', 'Year'], dtype={'Year':int})
measures = measures.merge(dunsmove, how='left', on='DunsYear')
del dunsmove

# subset file for years requested
measures = measures.loc[measures['Year'].isin(years)] 

#%% JOIN LOCATION INFO

dunslocs = pd.read_csv(dunslocation_file, sep='\t', usecols=['AddressID', 'DisplayX', 'DisplayY', 'UHCMatchCodeRank'])

measures = measures.merge(dunslocs, how='left', on='AddressID')
del dunslocs

#%% MERGE HIGH LEVEL CATEGORIES

xwalk = pd.read_csv(xwalk_file, sep='\t')

hl = measures.merge(xwalk[['BaseGroup', 'HighLevel']], on='BaseGroup')

#%% APPLY HIERARCHY IF APPLICABLE

if hierarchy == True:
    # join hierarchy
    measures = (measures
                   .merge(desc[['Category', 'Hierarchy']], left_on='BaseGroup', right_on='Category')
                   .drop(columns=['Category']))
    
    # sort by hierarchy, then drop all duplicates of dunsyear, keep first instance
    hierarchy_version = (measures
                         .sort_values(by='Hierarchy')
                         .drop_duplicates(subset=['DunsYear', 'Year'], keep='first'))
else: 
    pass


#%% ADD FOR LOOP THROUGH YEARS TO SPLIT INTO SEPARATE YEAR FILES 

for year in years:
    print(f'working on: {year}')
    temp = measures.loc[measures['Year'] == year]
    output_file = os.path.join(output_folder, f'beddn_businesslevel_measures{year}.txt')
    temp.to_csv(output_file, sep='\t', index=False)

toc = time.perf_counter()
t = toc - tic
print(f'total time: {t/60} minutes')