# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 11:33:55 2023

@author: stf45


"""

#%%
import pandas as pd

#%%

classified_long = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classified_long20230418.txt', sep='\t',  
                          # nrows=500000,
                          header=0,
                          # chunksize=chunksize
                         )
#%% DATA CHECKS
    
# get # of records
rownum = pd.DataFrame([len(classified_long)], columns=['n'])
unique_dunsyears = classified_long['DunsYear'].nunique()
unique_dunsyears_df = pd.DataFrame([unique_dunsyears], columns=['n'])
# get sum of each record's total category count, then:
#get unique values to show how many records were not categorized (0), categorized once (1), etc
catcounts = classified_long.groupby(['DunsYear']).count().reset_index().rename(columns={'BaseGroup':'catcount'})
unique_catcounts = pd.DataFrame(catcounts['catcount'].value_counts()).reset_index().rename(columns={'index':'BaseGroup_Counts','catcount':'Count'})
# get subset of all records categorized in > 2 categories, show cats
triplecats = catcounts.loc[catcounts['catcount'] > 2]
# get sum of each category's total record count
cat_freqs = classified_long['BaseGroup'].value_counts().reset_index().rename(columns={'index':'BaseGroup','BaseGroup':'count'}).sort_values('BaseGroup')

#%% WRITE EXCEL REPORT

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch/NETS_classified_long_report20230419.xlsx') as writer:
    cat_freqs.to_excel(writer, sheet_name='Classified DYs Per Cat', index=False)
    unique_catcounts.to_excel(writer, sheet_name='Cats Per DY Uniques', index=False)    
    triplecats.to_excel(writer, sheet_name='List of DYs in >2 Cats', index=False)
    rownum.to_excel(writer, sheet_name='Number of Records', index=False)
    unique_dunsyears_df.to_excel(writer, sheet_name='Unique DunsYears', index=False)
    
