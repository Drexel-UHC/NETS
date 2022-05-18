# -*- coding: utf-8 -*-
"""
Created on Mon May 16 09:24:21 2022

@author: stf45

This script grabs all records in the NETS dataset with "SENIOR CENTER" in the 
Company variable. Then it takes 5 of the top 6 freq SIC codes and pulls all 
records with these SICs from the most recent year available. This is used to check the potential
for an entirely new category for senior centers, and the accuracy of SIC code


Inputs: D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt

Outputs:
    senior_sic_check_20220516.xlsx
    snr_cntr_check.txt
"""
#%%

import pandas as pd
import time

company_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype=object,  header=0)
company = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype=object, header=0, usecols=['DunsNumber', 'Company', 'TradeName'], encoding_errors='replace')
sic = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype=object, header=0, usecols=['DunsNumber', 'SIC8'], encoding_errors='replace')

#%% MERGE FUNCTION

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = pd.merge(misc_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%% SEARCH FOR "SENIOR CENTER" IN COMPANY 

# do string search, grab dunsnumbers, find most recent sics of those dunsnumbers
# create new dataframe with the company names, dunsnumbers, and sics
snr_cntr = company[company['Company'].str.contains("SENIOR CENTER")]
snr_cntr_duns = snr_cntr.DunsNumber.tolist()
snr_cntr_sic = sic[sic['DunsNumber'].isin(snr_cntr_duns)]
senior_centers = pd.merge(snr_cntr, snr_cntr_sic, on='DunsNumber')

# get freqs for most recent sic codes, 
counts = snr_cntr_sic.SIC8.value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC8', 'sic_counts']


# these are five of the top six sic codes counted from dunsnumbers with "Company"
#that matched 'SENIOR CENTER'. The remaining of the top six is "amusement and 
#recreation services, nec. perhaps check this sic with string search, or all sics 
#with string search
sic_list = ['83220103', '83220000', '83220100', '83990100', '83220101']
sics_in_siclist = sic[sic['SIC8'].isin(sic_list)]
duns_in_siclist = sics_in_siclist.DunsNumber.tolist()


#%%
# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC8"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "EmpHere"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "SalesHere"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])
                                                                                                                                                          
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['DunsNumber'].isin(duns_in_siclist)]
    sic_check_wide = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk)
    sic_check_wide.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/snr_cntr_sics.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)                                                                                                

#%% REINSTANTIATE READERS

# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC8"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "EmpHere"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "SalesHere"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude", "FipsCounty"])
                 
#%% GET NAME SEARCH DUNSNUMBERS WITH LAT/LONGS AND OTHER ATTRIBUTES

readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['DunsNumber'].isin(snr_cntr_duns)]
    sic_check_wide = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk)
    sic_check_wide.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/snr_cntr_check.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)                                                                                                

#%% READ IN CSVS

snr_cntr_name = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/data_checks/snr_cntr_check.txt", sep = '\t', dtype={"DunsNumber": str},  header=0)
snr_cntr_sics = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/data_checks/snr_cntr_sics.txt", sep = '\t', dtype={"DunsNumber": str},  header=0)

#%% WRITE TABLE TO EXCEL

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\senior_sic_name_check_20220518.xlsx') as writer:
    snr_cntr_sics.to_excel(writer, "records w snr cntr sics", index=False)
    snr_cntr_name.to_excel(writer, "records w SENIOR CENTER", index=False)
    counts.to_excel(writer, "sic freqs from string search", index=False)