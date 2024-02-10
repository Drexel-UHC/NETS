# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 14:09:16 2023

@author: stf45


This file generates the CategoryDescriptions and BG_CC_TC_Xwalk files used in
the NETS2022/BEDDN2022 database.
"""

import pandas as pd


## ADJUSTING THIS TO JOIN THE HIERARCHY COLUMN TO CATEGORY DESCRIPTIONS TABLE.
#%%

# Read excel file with sheet name
dict_df = pd.read_excel(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Documentation\SIC_Code_Sorting_JAH/NETS_Catalogue_3LTR_Variables_20231023.xlsx', sheet_name=['NETS_Catalogue_3LTR_Code','Current Matrix', 'Hierarchy'])

# Get DataFrame from Dict
desc = dict_df.get('NETS_Catalogue_3LTR_Code')
desc = desc.iloc[1:,:4]
desc.columns = ['CategoryLong', 'Category', 'Domain', 'Type']
hierarchy = dict_df.get('Hierarchy')
hierarchy = hierarchy.rename(columns={'Code':'Category','Order in Hierarchy':'Hierarchy'})
hierarchy = hierarchy[['Category','Hierarchy']]
desc = desc.merge(hierarchy, how='left')


matrix = dict_df.get('Current Matrix')
matrix = matrix.iloc[1:]
matrix = matrix.drop(columns='Unnamed: 1')
matrix = matrix.rename(columns={'Unnamed: 0':'BaseGroup'})
matrix_long = pd.melt(matrix, id_vars=['BaseGroup'])
matrix_long = matrix_long.loc[matrix_long['value']==1]
matrix_long = matrix_long.drop(columns='value')
matrix_long = matrix_long.rename(columns={'variable':'HighLevel'})
matrix_long = matrix_long.sort_values(['BaseGroup', 'HighLevel'])

# create unique int id for highlevel values
matrix_long['BGHighLevelID'] = range(1, (len(matrix_long)+1))

matrix_long = matrix_long[['BGHighLevelID','BaseGroup','HighLevel']]

#%% EXPORT TABLES TO TXT

desc.to_csv(r'D:\scratch\CategoryDescriptionsYYYYMMDD.txt', sep='\t', index=False)
matrix_long.to_csv(r'D:\scratch\BG_CC_TC_XwalkYYYYMMDD.txt', sep='\t', index=False)
