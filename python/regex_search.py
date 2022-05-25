# -*- coding: utf-8 -*-
"""
Created on Wed May 25 09:54:25 2022

@author: stf45

This script does a regex search for senior citizens centers in the NETS
dataset and exports all records with relevant columns into a csv "check_cash_name.txt".
It will likely be replaced with a more complex and inclusive regex search in a 
different file.

Inputs: D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt
    
    C:\Users\stf45\Documents\NETS\Processing\config\
    regex_config.json

Outputs:
    social_serv_check_20220516.xlsx
    social_serv_check.txt
    
Runtime: approx 8 mins
"""

#%%

import pandas as pd
import time
import json
from datetime import datetime

with open('../config/regex_config.json', 'r') as f:
    config = json.load(f)
#%% MERGE FUNCTION

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = pd.merge(misc_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%% LOAD IN FILES AS READERS

# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC8"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str, "ZipCode": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "EmpHere"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "SalesHere"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str, "FipsCounty":str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])

# # SAMPLE FILES:           
# n = 1000
# chunksize = 100

# sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                               "SIC8"])

                                                                                                                                                          
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
#                                                                                                                                                                     "Company",
#                                                                                                                                                                     "TradeName",
#                                                                                                                                                                     "Address",
#                                                                                                                                                                     "City",
#                                                                                                                                                                     "State",
#                                                                                                                                                                     "ZipCode"])
# emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "EmpHere"])

                                                                                                                                                                                  

# sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "SalesHere"])
                      
# misc_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])
                                                                                                                                                          
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

print(f"Start Time: {datetime.now()}")
readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    # do string search, grab dunsnumbers, find most recent sics of those dunsnumbers
    # create new dataframe with the company names, dunsnumbers, and sics
    snr_cntr = '|'.join(config["SNR"]['name'])
    company_match = company_chunk[(company_chunk['Company'].str.contains(regex)) | (company_chunk['TradeName'].str.contains(regex))]
    out_df = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_match, misc_chunk)
    # add leading zeros to 
    out_df['Longitude'] = out_df['Longitude']*-1
    out_df.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/regex_search.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)                                                                                                

#%%
regex_search_csv = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/regex_search.txt", sep = '\t', dtype=str,  header=0)


with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\regex_search.xlsx') as writer:
    regex_search_csv.to_excel(writer, "regex search results", index=False)

