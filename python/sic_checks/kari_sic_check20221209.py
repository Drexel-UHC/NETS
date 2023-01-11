# -*- coding: utf-8 -*-
"""
Created on Fri Dec  9 10:20:04 2022

@author: stf45

Inputs: 
D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt
    
C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20220817\
    SIC_Check08172022.csv (a csv file containing SICs in question with official SIC descriptions
                           and comments made by kari)

Outputs: 
C:\Users\stf45\Documents\NETS\Processing\data_checks\from_kari_20220817\
    kari_sic_check20220817.txt
    kari_sic_check20220817.xlsx
        sheet1: sic search results
        sheet2: sic freqs
        
Runtime: approx 14 mins
"""
#%%
import pandas as pd
import time
import json
from datetime import datetime


#%% NEW CONSOLIDATED LIST OF SICS TO CHECK

sics = '''59999913
50470306
50489901
50489902
50489903
59999901
50499905
'''

siclist = sics.splitlines()

#%% MERGE FUNCTION

# function used to merge all relevant NETS files/variables into one dataframe

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = misc_merge.merge(sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%% LOAD IN FILES AS READERS

# # SAMPLE FILES
# n = 1000
# chunksize = 100

# sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str, 'SIC19': str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                               "SIC19"])

# emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                               "Emp19"])

# sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",       
#                                                                                                                                                                                   "Sales19"])
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                                     "Company",
#                                                                                                                                                                                     "TradeName",
#                                                                                                                                                                                     "Address",
#                                                                                                                                                                                     "City",
#                                                                                                                                                                                     "State",
#                                                                                                                                                                                     "ZipCode"])
# misc_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt', sep = '\t', dtype={"DunsNumber":str},  header=0, chunksize=chunksize, usecols=["DunsNumber", "Latitude", "Longitude"])



# FULL FILES:   load in files 10,000,000 records at a time         
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str, "SIC19": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC19"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Emp19"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Sales19"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])
                                                                                                                                                          
                                                                                                                                        
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV "kari_sic_check20221209.txt" IN CHUNKS

cols = ['DunsNumber', 'Company', 'TradeName', 'Address', 'City', 'State', 'ZipCode', 'SIC19', 'Emp19', 'Latitude', 'Longitude', 'Sales19']
print(f"Start Time: {datetime.now()}")

# run loaded files (chunks) through loop to grab all sics in siclist and output
#to 

readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = [0]
tic = time.perf_counter()

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['SIC19'].isin(siclist)]
    sic_check_wide = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk)
    sic_check_wide = sic_check_wide[cols]
    print(list(sic_check_wide.columns))
    print(len(sic_chunk))
    sic_check_wide.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/kari_sic_check20221209.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% CHECK DATA

sics_records = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\kari_sic_check20221209.txt', sep = '\t', dtype={"DunsNumber": str})

#%% GET SIC FREQS

counts = sics_records.SIC19.value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC19', 'sic_counts']

#%% WRITE TO EXCEL 

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\kari_sic_check20221209.xlsx') as writer:
    sics_records.to_excel(writer, "SIC19 subset results", index=False)
    counts.to_excel(writer, "SIC freqs", index=False)