# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 09:45:53 2023

@author: stf45
"""

import pandas as pd

#%% ADDRESS

address = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Addresses20231027.txt', sep='\t', dtype={'GcZIP':str, 'GcZIP4':str})

address.dtypes

stats = address.describe()

check = address.loc[address['GcAddress'].isna()]

'''
notes:
GcZIP == 9999 (n=941)
GcAddress is na (n=72710)
'''

address['GcAddress'].str.len().max()

for col in address.columns:
    if type(col) == str:
        strmax = address[col].str.len().max()
        print(f"{col}: {strmax}")
    else: 
        pass
    
#%% CLASSIFIED LONG

classifiedlong = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\ClassifiedLongDB20230822.txt', sep='\t')

# get # of records
rownum = pd.DataFrame([len(classifiedlong)], columns=['n'])
unique_dunsyears = classifiedlong['DunsYear'].nunique()
unique_dunsyears_df = pd.DataFrame([unique_dunsyears], columns=['n'])
# get sum of each record's total category count, then:
#get unique values to show how many records were not categorized (0), categorized once (1), etc
catcounts = classifiedlong.groupby(['DunsYear']).count().reset_index().rename(columns={'BaseGroup':'catcount'})
unique_catcounts = pd.DataFrame(catcounts['catcount'].value_counts()).reset_index().rename(columns={'index':'BaseGroup_Frequency','catcount':'Count'})
# get subset of all records categorized in > 2 categories, show cats
triplecats = catcounts.loc[catcounts['catcount'] > 2]
# get sum of each category's total record count
cat_freqs = classifiedlong['BaseGroup'].value_counts().reset_index().rename(columns={'index':'BaseGroup','BaseGroup':'count'}).sort_values('BaseGroup')

##

with pd.ExcelWriter(r'D:\scratch/NETS_classified_long_report.xlsx') as writer:
    cat_freqs.to_excel(writer, sheet_name='Classified DYs Per Cat', index=False)
    unique_catcounts.to_excel(writer, sheet_name='Cats Per DY Uniques', index=False)    
    triplecats.to_excel(writer, sheet_name='List of DYs in >2 Cats', index=False)
    rownum.to_excel(writer, sheet_name='Number of Records', index=False)
    unique_dunsyears_df.to_excel(writer, sheet_name='Unique DunsYears', index=False)
    
    


