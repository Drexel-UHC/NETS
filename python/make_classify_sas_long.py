# -*- coding: utf-8 -*-
"""
Created on Fri May 26 15:57:34 2023

@author: stf45

This is a script to convert the Classified Dataset (SAS) (classified_SASYYYYMMDD.txt) from wide to long
and to merge it with the Classified Dataset Long (Python) (ClassifiedLongYYYYMMDD.txt)

runtime: ~15mins
"""
#%% 
import pandas as pd

#%% READ CLASSIFIED DATASET (SAS) FILE
    
classified = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classified_SAS20231018.txt', sep = '\t', encoding_errors='replace', header=0)

print(f'nrows of classified sas: {len(classified)}')

#%% CONVERT TO LONG AND APPEND TO CLASSIFIED LONG DATASET (PYTHON)

# columns will be DunsYear and Basegroup. neither column is unique on its own.
melted = pd.melt(classified, id_vars=['DunsYear'], var_name='BaseGroup')
melted = melted.loc[melted['value']==1]
melted = melted.drop(columns=['value'])
melted.to_csv(r"D:/scratch/ClassifiedLong20231024.txt", sep="\t", header=False, mode='a', index=False)

print(len(melted))
del classified

#%% DATA CHECK

# CHECK TO SEE IF N OF UNIQUE DUNSYEARS IN CLASSIFED SAS == N OF UNIQUE DUNSYEARS IN CLASSIFICATION SAS
dups = melted.loc[melted.duplicated(subset=['DunsYear'], keep=False)]
print(melted.nunique())

#%% GET VALUE COUNTS AND REFORMAT

counts = melted['BaseGroup'].value_counts()
counts.loc['total']= counts.sum()
counts = counts.reset_index().rename(columns={'index':'BaseGroup', 'BaseGroup':'Count'}).sort_values(by='BaseGroup')

#%% WRITE EXCEL REPORT

with pd.ExcelWriter(r'D:/scratch/NETS_classify_long_reportYYYYMMDD.xlsx') as writer:
    counts.to_excel(writer, sheet_name='cat_counts', index=False)    

