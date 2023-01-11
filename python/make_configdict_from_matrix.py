# -*- coding: utf-8 -*-
"""
Created on Wed Dec 21 15:19:50 2022

@author: stf45
"""
#%%
import pandas as pd
import json
#%% LOAD MATRIX, CONVERT TO LONG

matrix = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\config\main_cat_matrix.csv')
matrix = matrix.rename(columns = {'Unnamed: 0': 'aux'})
matrix = matrix.drop(columns=['Unnamed: 1'])

matrix_long = pd.melt(matrix, id_vars=['aux'])
matrix_long = matrix_long.loc[matrix_long['value']==1]
matrix_long = matrix_long.drop(columns='value')
matrix_long = matrix_long.rename(columns={'variable':'main'})
matrix_long = matrix_long.sort_values(['aux', 'main'])

#%% EXPORT LONG VERSION TO CSV

matrix_long.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\main_aux_link20221222', index=False)

#%% COPY AND ADD MAIN LEAD COLUMN

matrix_long_lead = matrix_long.copy()
matrix_long_lead = matrix_long_lead.sort_values(['main', 'aux'])
matrix_long_lead['lead'] = matrix_long_lead['main'].shift(-1)

#%% CREATE DICT OF DICTS TO CONVERT TO JSON

# creates dictionary of dictionaries in order to export to json config:
  #  {
  #    'AAL': {
  #       'auxiliary': [
  #          'BAR', 
  #          'LIQ'
  #          ]
  #    }
  # }

mdict={}
adict={}
alist=[]

for row in matrix_long_lead.iterrows():
    aux = row[1][0]
    main = row[1][1]
    lead = row[1][2]
    if main == lead:
        alist.append(aux)
    else:
        alist.append(aux)
        adict['auxiliary'] = alist 
        mdict[main] = adict
        adict={}
        alist=[]
        
        
#%%

mdict2={}
alist2=[]

for row in matrix_long_lead.iterrows():
    aux = row[1][0]
    main = row[1][1]
    lead = row[1][2]
    if main == lead:
        alist2.append(aux)
    else:
        alist2.append(aux) 
        mdict2[main] = alist2 
        alist2=[]

main_aux_wide = pd.DataFrame.from_dict(dict([ (k,pd.Series(v)) for k,v in mdict2.items() ]), orient='index')

pd.DataFrame(mdict2)

#%% EXPORT TO JSON



with open(r"C:\Users\stf45\Documents\NETS\Processing\scratch\main_cat_config20221221.json","w") as outfile:
    json.dump(mdict, outfile, indent=2)