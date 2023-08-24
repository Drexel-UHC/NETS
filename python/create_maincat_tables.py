# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 14:09:16 2023

@author: stf45
"""

import pandas as pd

# Read excel file with sheet name
dict_df = pd.read_excel(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Documentation\SIC_Code_Sorting_JAH/NETS_Catalogue_3LTR_Variables_20230516.xlsx', sheet_name=['NETS_Catalogue_3LTR_Code','Current Matrix'])

# Get DataFrame from Dict
desc = dict_df.get('NETS_Catalogue_3LTR_Code')
desc = desc.iloc[1:,:4]
desc.columns = ['CategoryLong', 'Category', 'Domain', 'Type']

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

desc.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\category_descYYYYMMDD.txt', sep='\t', index=False)
matrix_long.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\BG_CC_TC_XwalkYYYYMMDD.txt', sep='\t', index=False)
