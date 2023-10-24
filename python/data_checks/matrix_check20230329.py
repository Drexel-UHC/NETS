# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 17:33:17 2023

@author: stf45
"""
#%%
import os 
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import pandas as pd
import re
import json
import nets_functions as nf

#%%

##### PART 1: CHECK NETS_CATALOGUE_3LTR_CODE WITH MATRIX ###############################################
'''
Check that all of the 3 letter codes in “NETS_Catalogue_3LTR_Code” are 
included in “Matrix” and vice versa
'''
#%% LOAD MATRIX, CONVERT TO LONG

# just paste matrix into new excel csv and remove description column, no need to rename columns in excel
matrix = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\config\main_cat_matrix20230612.csv')
matrix.columns.values[0] = 'aux'
matrix_long = pd.melt(matrix, id_vars=['aux'])
matrix_long = matrix_long.loc[matrix_long['value']==1]
matrix_long = matrix_long.drop(columns='value')
matrix_long = matrix_long.rename(columns={'variable':'main'})
matrix_long = matrix_long.sort_values(['aux', 'main'])

#%% CHECK NETS_CATALOGUE_3LTR_CODE CATS WITH MATRIX WORKSHEET CATS (MAINS AND AUXS)

# list mains and auxs from matrix
matrix_aux = set(matrix['aux'].unique()) #184
matrix_main = set(matrix.columns.unique()) 
matrix_main.remove('aux') #71

# list mains and auxs from NETS_Catalogue_3LTR_Code
## make sure columns are "cat" and "type"
ltrcats = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\config\3ltr_cats20230329.csv', encoding_errors='replace')
ltr_main = set(ltrcats['cat'].loc[ltrcats['type'].isin(['Main','Intermediate'])]) #71
ltr_aux = set(ltrcats['cat'].loc[ltrcats['type'] == 'Auxiliary']) #184

# aux cats in 3ltr but not matrix:
print(ltr_aux.difference(matrix_aux))
# aux cats in matrix but not 3ltr: 
print(matrix_aux.difference(ltr_aux))
# main cats in 3ltr but not matrix: 
print(ltr_main.difference(matrix_main)) 
# main cats in matrix but not 3ltr:
print(matrix_main.difference(ltr_main))

####### PART 2: CHECK TECH DEFS WITH MATRIX ############################################################
'''
Main/Intermediate - Check that the coding in “NETS_Catalogue_3LTR_Code” matches 
the categories assigned in “Matrix” sheet
'''
#%% CREATE WIDE DF OF MATRIX (STRINGS)

mcopy = matrix_long.copy()
mcopy['aux'] = mcopy.groupby(['main'])['aux'].transform(lambda x : ' '.join(x))
mcopy = mcopy.drop_duplicates()
matrix_wide = mcopy[['main','aux']]
matrix_wide['source'] = 'matrix'

#%% LOAD TECH DEFS FOR MAINS

# make sure column name for cats is "cat"
techdef = ltrcats[['cat','techdef']].loc[ltrcats['type'].isin(['Main','Intermediate'])].reset_index(drop=True)
regex = "\\b(?<!then )[A-Z]{3}(?= =)\\b"

techdef2 = []
for x in techdef.iterrows():
    match = re.findall(regex,x[1][1])
    match.sort()
    matchstr = " ".join(match)
    matchsort = matchstr
    techdef2.append(matchstr)


techdefseries = pd.Series(techdef2, name='aux')
techdef3 = pd.concat([techdef,techdefseries], axis=1)
techdef3 = techdef3.drop(columns=['techdef'])
techdef3 = techdef3.sort_values(by='cat')
techdef3 = techdef3.rename(columns={'cat': 'main'})
techdef3['source'] = 'techdef'

#%% LIST DIFFERENCES BETWEEN MATRIX AND TECHDEF

diff = pd.concat([techdef3,matrix_wide]).drop_duplicates(subset=['main','aux'], keep=False)
diff = diff.sort_values(by='main')

for c,x in enumerate(diff.iterrows()):
    deflist=[]
    if c%2==0:
        seta = set(x[1][1].split())   
        sourcea = x[1][2]
        cat = x[1][0]
    else:
        setb = set(x[1][1].split())
        sourceb = x[1][2]
        diffa = seta.difference(setb)
        diffb = setb.difference(seta)
        print(f"{cat}: {diffa} in {sourcea} but not {sourceb}")
        print(f"{cat}: {diffb} in {sourceb} but not {sourcea}")

####### PART 3: CHECK TECH DEFS WITH "SIC Codes" SHEET ############################################################
'''
Auxiliary - Check that the coding in “NETS_Catalogue_3LTR_Code” matches 
the categories assigned in “SIC Codes” columns “SIC Only Auxilary Code 1”, 
“SIC Only Auxilary Code 2”, “SIC Only Auxilary Code 3”
'''

#%% LOAD JSON CONFIG

with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230227.json', 'r') as f:
    config = json.load(f)

sicsheet = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\config\siccodes_sheet20230329.csv')
sicsheet = sicsheet.fillna('')

# 'SIC Only Auxilary Code 1'
sicsheet_code1 = sicsheet.loc[sicsheet['SIC Only Auxilary Code 1'].str.contains('[A-Z]{3}')]

sicsheet_code1['SICCode'] = sicsheet_code1['SICCode'].astype(str)
sicsheet_code1['sics'] = sicsheet_code1[['SIC Only Auxilary Code 1','SICCode']].groupby(['SIC Only Auxilary Code 1'])['SICCode'].transform(lambda x: ','.join(x))
sicsheet_code1 = sicsheet_code1.drop_duplicates('SIC Only Auxilary Code 1')
sicsheet_code1 = sicsheet_code1[['SIC Only Auxilary Code 1','sics']]

# 'SIC Only Auxilary Code 2' (just BDS)
sicsheet_code2 = sicsheet.loc[sicsheet['SIC Only Auxilary Code 2'].str.contains('[A-Z]{3}')]

sicsheet_code2['SICCode'] = sicsheet_code2['SICCode'].astype(str)
sicsheet_code2['sics'] = sicsheet_code2[['SIC Only Auxilary Code 2','SICCode']].groupby(['SIC Only Auxilary Code 2'])['SICCode'].transform(lambda x: ','.join(x))
sicsheet_code2 = sicsheet_code2.drop_duplicates('SIC Only Auxilary Code 2')
sicsheet_code2 = sicsheet_code2[['SIC Only Auxilary Code 2','sics']]

code2 = list(sicsheet_code2['sics'].str.split(","))

# 'SIC Only Auxilary Code 3' (just GRY)
sicsheet_code3 = sicsheet.loc[sicsheet['SIC Only Auxilary Code 3'].str.contains('[A-Z]{3}')]

sicsheet_code3['SICCode'] = sicsheet_code3['SICCode'].astype(str)
sicsheet_code3['sics'] = sicsheet_code3[['SIC Only Auxilary Code 3','SICCode']].groupby(['SIC Only Auxilary Code 3'])['SICCode'].transform(lambda x: ','.join(x))
sicsheet_code3 = sicsheet_code3.drop_duplicates('SIC Only Auxilary Code 3')
sicsheet_code3 = sicsheet_code3[['SIC Only Auxilary Code 3','sics']]

code3 = list(sicsheet_code3['sics'].str.split(","))

#%% COMPARE SICS/CATS IN SICSHEET AND 3LTR SHEET
        
numrows = len(sicsheet_code1)

# aux cats in config but not sicsheet
catdiff = set(config.keys()).difference(set(sicsheet_code1['SIC Only Auxilary Code 1']))
print(catdiff)
# aux cats in sicsheet but not config
catdiff2 = set(sicsheet_code1['SIC Only Auxilary Code 1']).difference(set(config.keys()))
print(catdiff2)

for c,row in enumerate(sicsheet_code1.iterrows()):
    cat = row[1][0]
    sheetsics = set(row[1][1].split(","))
    
    if cat in config.keys():
        if config[cat]['conditional'] in [2,3,6,10,13,14,15]:
            sics = set(map(str,nf.make_sic_range(cat,config)))
            diff = sheetsics.difference(sics)
            if len(diff) != 0:
                print(cat, diff)
            else:
                pass
        elif config[cat]['conditional'] in [7,12]:
            sics = set(map(str,nf.make_sic_range2(cat,config)))
            diff = sheetsics.difference(sics)
            if len(diff) != 0:
                print(cat, diff)
            else:
                pass
        elif config[cat]['conditional'] in [4]:
            sics = set(map(str,nf.make_sic_ex_range(cat,config)))
            diff = sheetsics.difference(sics)
            if len(diff) != 0:
                print(cat, diff)
            else:
                pass
        elif config[cat]['conditional'] in [8,9]:
            sics = set(map(str,config[cat]['sic_exclusive']))
            diff = sheetsics.difference(sics)
            if len(diff) != 0:
                print(cat, diff)
            else:
                pass
        else:
            print(f'missing condit for {cat}')
    else:
        print(f"{cat} not in config")
        
        
#%% REMOVE NAME SEARCH CATS FROM SET PRODUCED IN ABOVE CELL


namecatset = {'BDS', 'GRY','GMN', 'SMN', 'CSD', 'FCS', 'CMN', 'DLR', 'BKN', 'WRN', 'CFN', 'DRN', 'CNG', 'QSV', 'SCT', 'CNN', 'SPN', 'PIN', 'AMW', 'CMW', 'LIN'}
print(f'final list of cats in config but missing from siccodes sheet: {catdiff.difference(namecatset)}')
        
