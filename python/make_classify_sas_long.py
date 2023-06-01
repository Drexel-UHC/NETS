# -*- coding: utf-8 -*-
"""
Created on Fri May 26 15:57:34 2023

@author: stf45

This is a script to convert the Classified Dataset (SAS) (classifiedYYYYMMDD.txt) from wide to long
and to merge it with the Classified Dataset Long (Python)


runtime: ~15mins
"""
#%% 
import pandas as pd

#%% READ CLASSIFIED DATASET (SAS) FILE
    
classified = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_sas20230526.txt', sep = '\t', encoding_errors='replace', header=0)

#%% CONVERT TO LONG AND APPEND TO CLASSIFIED LONG DATASET (PYTHON)

# columns will be DunsYear and Basegroup. neither column is unique on its own.
melted = pd.melt(classified, id_vars=['DunsYear'], var_name='BaseGroup')
melted = melted.loc[melted['value']==1]
melted = melted.drop(columns=['value'])
melted.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classified_long20230526.txt", sep="\t", header=False, mode='a', index=False)

len(melted)
del classified

#%% DATA CHECK

#CHECK TO SEE IF N OF CLASSIFED == N OF CLASSIFICATION SIC SUBSET 

classified_long = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_long20230526.txt', sep='\t', dtype = object)
dups = classified_long.loc[classified_long.duplicated(subset=['DunsYear'], keep=False)]
print(classified_long.nunique())

#%% GET VALUE COUNTS AND REFORMAT

counts = classified_long['BaseGroup'].value_counts()
counts.loc['total']= counts.sum()
counts = counts.reset_index().rename(columns={'index':'BaseGroup', 'BaseGroup':'Count'}).sort_values(by='BaseGroup')


#%% WRITE EXCEL REPORT

with pd.ExcelWriter(r'C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/NETS_classify_long_report20230530.xlsx') as writer:
    counts.to_excel(writer, sheet_name='cat_counts',index=False)    

