# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 09:06:34 2022

@author: stf45
"""

# -*- coding: utf-8 -*-
"""
Created on Wed May 25 09:54:25 2022

@author: stf45

This script does a regex search for dollar stores in the NETS 2019
dataset and exports all records with relevant columns into a csv "dollar_store_regex.txt",
which is also exported to an excel file dollar_store_regex20220722.xlsx.
It also gets counts for each regex group (dollar store name) match and outputs 
on a different sheet in the same excel file.


Inputs: D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt
    
    C:\Users\stf45\Documents\NETS\Processing\config\
    dollar_store_regex_config.json

Output: C:\Users\stf45\Documents\NETS\Processing\data_checks\
    check_cash_regex20220531.txt
    check_cash_regex20220531.xlsx
    
Runtime: approx 20 mins
"""

#%%

import pandas as pd
import time
import json
from datetime import datetime

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

with open(r'C:\Users\stf45\Documents\NETS\Processing\config/dollar_store_regex_config.json', 'r') as f:
    config = json.load(f)
#%% MERGE FUNCTION

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

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str, "SIC8": int},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
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
time_list = [0]

tic = time.perf_counter()

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    header = (c==0)
    # do string search, grab dunsnumbers, find most recent sics of those dunsnumbers
    # create new dataframe with the company names, dunsnumbers, and sics
    regex = '|'.join(config["DLR"]['name'])
    company_match = company_chunk[(company_chunk['Company'].str.contains(regex)) | (company_chunk['TradeName'].str.contains(regex))]
    out_df = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_match, misc_chunk)
    # make longs negative
    out_df['Longitude'] = out_df['Longitude']*-1
    out_df.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/dollar_store_regex.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))
    
runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)            

#%% DROP NANS FROM SIC19

dlrstore = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/dollar_store_regex.txt", dtype={'DunsNumber':str, 'ZipCode':str, 'FipsCounty':str}, sep = '\t',  header=0)

dlrstore.dropna(subset=['SIC19'], inplace=True)
dlrstore['SIC19'] = dlrstore['SIC19'].astype(int)
dlrstore['SIC19'] = dlrstore['SIC19'].astype(str)
dlrstore['SIC19'] = dlrstore['SIC19'].str.zfill(8)

#%%

# get freqs for most recent sic codes, 
counts = dlrstore.SIC19.value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC19', 'sic_counts']

#%% DOLLAR STORE NAMES

namelist = ['Dollar General',
'Family Dollar',
'Dollar Tree',
'Five Below',
'99 Cents Only Stores',
'DG Market',
'Daiso Japan',
'Popshelf',
'Maxway',
'DGX',
'King Dollar',
'Super Dollar',
'Just A Buck',
'Mighty Dollar',
'Super 10',
'Merchandise Outlet',
'Dollar Star',
'Bills',
'Dollar Castle',
'Dollar Daze',
'Hoys 5 & 10',
'Bargain Town USA',
'Cee & Cee Department Stores',
'Dollar Max',
'A Dollar']

namelist = pd.DataFrame({'store_name': namelist})
namelist.reset_index(drop=True, inplace=True)

#%%

dlrstore['regex_name'] = dlrstore['Company'] + dlrstore['TradeName']
dlrgroup= dlrstore['regex_name'].str.extract(regex)
dlrstore.drop(columns=['regex_name'], inplace=True)

#%% READ IN CSV FOR GROUP COUNTS, GET FREQS

# get freqs for dollar store names, 
store_counts = dlrgroup.notnull().sum()
store_counts = pd.DataFrame({'count': store_counts})
store_counts.reset_index(drop=True,inplace=True)
store_countsdf = pd.concat([namelist, store_counts], axis=1)   
store_countsdf['count'].sum()

#%% WRITE TO EXCEL 

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dollar_store_regex.xlsx') as writer:
    dlrstore.to_excel(writer, "regex search results", index=False)
    counts.to_excel(writer, "SIC freqs for regex search", index=False)
    store_countsdf.to_excel(writer, "store name counts", index=False)
