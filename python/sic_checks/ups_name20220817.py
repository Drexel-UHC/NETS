# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 15:24:58 2022

@author: stf45

This script does a regex search for UPS stores in the NETS 2019
dataset and exports all records with relevant columns into a csv "ups_name20220817.txt",
which is also exported to an excel file ups_name20220817.xlsx.

Inputs: 
D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt

Output: 
C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20220817\
    ups_name20220817.txt
    ups_name20220817.xlsx
    
Runtime: approx 20 mins
"""

#%%

import pandas as pd
import time
import json
from datetime import datetime
import warnings
# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

#%% MERGE AND SIC_RANGE FUNCTIONS

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = company_chunk.merge(sic_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = pd.merge(misc_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

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
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Emp19"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Sales19"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str, "FipsCounty":str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])

# # SAMPLE FILES:           
# n = 1000
# chunksize = 100

# sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str, "SIC19": int},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                               "SIC19"])

                                                                                                                                                          
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                     "Company",
#                                                                                                                                                                     "TradeName",
#                                                                                                                                                                     "Address",
#                                                                                                                                                                     "City",
#                                                                                                                                                                     "State",
#                                                                                                                                                                     "ZipCode"])
# emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Emp19"])

                                                                                                                                                                                  

# sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Sales19"])
                      
# misc_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])
         
                                                                                                                                                
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

print(f"Start Time: {datetime.now()}")
readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = [0]

tic = time.perf_counter()

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    header = (c==0)
    # do string search, grab dunsnumbers, find most recent sics of those dunsnumbers
    # create new dataframe with the company names, dunsnumbers, and sics
    sic_chunk.dropna(subset=['SIC19'], inplace=True)
    sic_chunk['SIC19'] = sic_chunk['SIC19'].astype(int)
    sic_chunk['SIC19'] = sic_chunk['SIC19'].astype(str)
    sic_chunk['SIC19'] = sic_chunk['SIC19'].str.zfill(8)
    company_match = company_chunk[(company_chunk['Company'].str.contains("\\b(UPS)\\b")) | (company_chunk['TradeName'].str.contains("\\b(UPS)\\b"))]
    out_df = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_match, misc_chunk)
    # make longs negative
    out_df['Longitude'] = out_df['Longitude']*-1
    out_df.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/ups_name20220817.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))
    
runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)            

#%% CHECK DATA

ups = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/ups_name20220817.txt", dtype={'DunsNumber':str, 'ZipCode':str, 'FipsCounty':str}, sep = '\t',  header=0)

#%%

# get freqs for sic codes, 
counts = ups.SIC19.value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC19', 'sic_counts']

#%% WRITE TO EXCEL 

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\ups_name20220817.xlsx') as writer:
    ups.to_excel(writer, "name search results", index=False)
    counts.to_excel(writer, "SIC freqs for name search", index=False)
