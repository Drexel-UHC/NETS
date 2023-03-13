# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 13:15:11 2023

@author: stf45

This script was created to find SIC frequencies from LIQ name search results in
order to create a new sic range for the category. output sent via email to Kari 
and Jana 02/16/2023


Inputs: D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    
    C:\Users\stf45\Documents\NETS\Processing\config\
    nets_config_20230206.json

Output: C:\Users\stf45\Documents\NETS\Processing\data_checks\
    cmu_name20230216.txt
    cmu_name20230216.xlsx
    
Runtime: 10 mins
"""

#%%

import pandas as pd
import time
import json
from datetime import datetime
import warnings 

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230206.json', 'r') as f:
    config = json.load(f)

#%% LOAD IN FILES AS READERS

# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC19"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str, "ZipCode": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])

# # SAMPLE FILES:           
# n = 1000
# chunksize = 100

# sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                               "SIC19"])

                                                                                                                                                          
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                     "Company",
#                                                                                                                                                                     "TradeName",
#                                                                                                                                                                     "Address",
#                                                                                                                                                                     "City",
#                                                                                                                                                                     "State",
#                                                                                                                                                                     "ZipCode"])
                                                                                                                                             
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS
print(f"Start Time: {datetime.now()}")
readers = zip(sic_reader, company_reader)
time_list = [0]
tic = time.perf_counter()

for c, (sic_chunk,company_chunk) in enumerate(readers):
    header = (c==0)
    # do string search, grab dunsnumbers, find most recent sics of those dunsnumbers
    # create new dataframe with the company names, dunsnumbers, and sics
    regex = '|'.join(config["LIQ"]['name'])
    sic_chunk.dropna(subset=['SIC19'], inplace=True)
    company_match = company_chunk[(company_chunk['Company'].str.contains(regex)) | (company_chunk['TradeName'].str.contains(regex))]
    out_df = company_match.merge(sic_chunk, on='DunsNumber')

    out_df.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/liq_name20230217.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)                                                                                                

#%%
results = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/liq_name20230217.txt", sep = '\t', dtype={'DunsNumber':str,'ZipCode':str,'SIC19':int},  header=0)

descrip = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/config/sic_descriptions.csv", dtype={'SICCode':str}, header=0)

# get freqs for most recent sic codes, 
counts = results['SIC19'].value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC19', 'sic_counts']
counts['SIC19'] = counts['SIC19'].astype(int).astype(str).str.zfill(8) 

out_df = counts.merge(descrip, left_on="SIC19", right_on="SICCode")
out_df = out_df.drop(columns=['SICCode'])
#%%
with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\liq_name20230216.xlsx') as writer:
    results.to_excel(writer, "liq name search results", index=False)
    out_df.to_excel(writer, "sic freqs", index=False)
