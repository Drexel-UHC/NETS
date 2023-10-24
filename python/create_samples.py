# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 09:00:33 2023

@author: stf45
"""

import pandas as pd
import random
from datetime import datetime
import time

#%% GET RANDOM LIST OF NUMBERS

rand = random.sample(range(17512936), 200)

#%% COMPANY FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 17512936
n=87564680
company = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Company.txt', sep='\t', encoding_errors='replace', dtype={'DunsNumber':str}, chunksize=chunksize)

for c, chunk in enumerate(company):
    header = (c==0)
    sample = chunk.iloc[rand]
    sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\company_sampleUNSORTED.txt', index=False, sep='\t', mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime) 

# read in company sample unsorted, sort, then export tocsv
company_sample = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\company_sampleUNSORTED.txt', sep='\t', dtype={'DunsNumber':str})
company_sample = company_sample.sort_values('DunsNumber')
company_sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\company_sample.txt', index=False, sep='\t') 

#%% READ IN COMPANY SAMPLE

company_sample = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\company_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%% EMP FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 17512936
n=87564680
emp = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Emp.txt', sep='\t', encoding_errors='replace', dtype={'DunsNumber':str}, chunksize=17512936)

for c, chunk in enumerate(emp):
    header = (c==0)
    sample = chunk.loc[chunk['DunsNumber'].isin(company_sample['DunsNumber'])]
    sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\emp_sample.txt', index=False, sep='\t', mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% READ IN EMP SAMPLE

emp_sample= pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\emp_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%% MISC FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 17512936
n=87564680
misc = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Misc.txt', sep='\t', encoding_errors='replace', dtype={'DunsNumber':str}, chunksize=17512936)

for c, chunk in enumerate(misc):
    header = (c==0)
    sample = chunk.loc[chunk['DunsNumber'].isin(company_sample['DunsNumber'])]
    sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\misc_sample.txt', index=False, sep='\t', mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% READ IN MISC SAMPLE

misc_sample= pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\misc_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%% MOVE FILE

move = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Move.txt', sep='\t', encoding_errors='replace', dtype={'DunsNumber':str})

move_sample = move.loc[move['DunsNumber'].isin(company_sample['DunsNumber'])]

move_sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\move_sample.txt', index=False, sep='\t', mode='a', header=header)

#%% READ IN MOVE SAMPLE

move_sample= pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\move_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%% SALES FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 17512936
n=87564680

sales = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Sales.txt', sep='\t', encoding_errors='replace', chunksize=17512936, dtype={'DunsNumber':str}, low_memory=False)

for c, chunk in enumerate(sales):
    header = (c==0)
    sample = chunk.loc[chunk['DunsNumber'].isin(company_sample['DunsNumber'])]
    sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\sales_sample.txt', index=False, sep='\t', mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% READ IN SALES SAMPLE

sales_sample= pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\sales_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%% SIC FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 17512936
n=87564680
sic = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_SIC.txt', sep='\t', encoding_errors='replace', dtype={'DunsNumber':str}, chunksize=17512936)

for c, chunk in enumerate(sic):
    header = (c==0)
    sample = chunk.loc[chunk['DunsNumber'].isin(company_sample['DunsNumber'])]
    sample.to_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\sic_sample.txt', index=False, sep='\t', mode='a', header=header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% READ IN SIC SAMPLE

sic_sample= pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Samples\sic_sample.txt', sep='\t', dtype={'DunsNumber':str})

#%%

mreset = misc_sample['DunsNumber'].sort_values().reset_index() 
creset = company_sample['DunsNumber'].sort_values().reset_index()

diff = mreset['DunsNumber'].compare(creset['DunsNumber'])

check =misc_sample.DunsNumber.str.len() 
