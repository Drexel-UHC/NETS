# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:20:40 2023

@author: stf45

runtime: ~100 mins
"""
#%%
import pandas as pd
import time
from datetime import datetime
#%%

chunksize = 5000000
n = 353116375
classified_reader = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classified20230410.txt', sep='\t',  
                          # nrows=30000,
                          header=0,
                          chunksize=chunksize
                         )

#%%
rownum = []
cat_freqs_df = pd.DataFrame()
unique_catcounts_df = pd.DataFrame()
triplecats_df = pd.DataFrame()

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, class_chunk in enumerate(classified_reader):
    header = (c==0)
    # get # of records
    rownum.append(len(class_chunk))
    # get sum of each record's total category count, then:
    #get unique values to show how many records were not categorized (0), categorized once (1), etc
    check = pd.DataFrame()
    check['DunsYear'] = class_chunk.iloc[:,0:1]
    check['cats_per_dunsyear'] = class_chunk.iloc[:,1:].sum(axis=1)
    unique_catcounts = check['cats_per_dunsyear'].value_counts()
    unique_catcounts = pd.DataFrame(unique_catcounts)
    unique_catcounts_df = pd.concat([unique_catcounts_df,unique_catcounts], axis=1)
    # get subset of all records categorized in > 2 categories, show cats
    triplecats = check.loc[check['cats_per_dunsyear'] > 2]
    triplecats_merge = triplecats.merge(class_chunk, on='DunsYear')
    triplecats_long = pd.melt(triplecats_merge, id_vars=['DunsYear'], var_name='BaseGroup')
    triplecats_long = triplecats_long.loc[triplecats_long['value']==1]
    triplecats_long = triplecats_long.drop(columns=['value'])
    triplecats_df = pd.concat([triplecats_df,triplecats_long])
    # get sum of each category's total record count
    cat_freqs = class_chunk.iloc[:,1:].sum(axis=0)
    cat_freqs = pd.DataFrame(cat_freqs, columns=['count'])
    cat_freqs_df = pd.concat([cat_freqs_df,cat_freqs],axis=1)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))    
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

del check

# get sum of records of all chunks
rownumtotal = pd.DataFrame([sum(rownum)], columns=['n'])

# format cat_freqs table
cat_freqs_final = cat_freqs_df.sum(axis=1).reset_index()
cat_freqs_final = pd.DataFrame(cat_freqs_final).rename(columns={'index':'Category', 0:'Count'})
del cat_freqs_df

# format unique cat counts table
unique_catcounts_final = unique_catcounts_df.sum(axis=1).reset_index().rename(columns={'index':'BaseGroup_Counts', 0:'Count'})

# get 
triplecats_final = pd.DataFrame(triplecats_df['DunsYear'].unique()).rename(columns={0:'DunsYear'})
#%% WRITE EXCEL REPORT

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch/NETS_classify_report20230418.xlsx') as writer:
    cat_freqs_final.to_excel(writer, sheet_name='Classified DYs Per Cat', index=False)
    unique_catcounts_final.to_excel(writer, sheet_name='Cats Per DY Uniques', index=False)    
    triplecats_final.to_excel(writer, sheet_name='List of DYs in >2 Cats', index=False)
    rownumtotal.to_excel(writer, sheet_name='Number of Records', index=False)