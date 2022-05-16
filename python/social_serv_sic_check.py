# -*- coding: utf-8 -*-
"""
Created on Mon May 16 11:25:31 2022

@author: stf45
"""

# -*- coding: utf-8 -*-
"""
Created on Mon May 16 11:05:02 2022

@author: stf45
"""

import pandas as pd
import time


#%% MERGE FUNCTION

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = pd.merge(misc_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%%
# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str, 'SIC19':str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
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
                                                                                                                                                          
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['SIC19'].str.startswith('8399', na=False)]
    sic_check_wide = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk)
    sic_check_wide.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/social_serv_check.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)     


#%% READ IN CSV

social_serv = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/social_serv_check.txt", sep = '\t', dtype={"DunsNumber": str},  header=0)


#%% WRITE TABLE TO EXCEL

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\social_serv_check_20220516.xlsx') as writer:
    social_serv.to_excel(writer, "records w sic 8399 (2019)", index=False)



