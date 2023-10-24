# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022
edited 10/16/2023 for NETS2022

@author: stf45

This script takes the full company, sic, emp, and sales files and pivots them
into a long file to be used as the input for classify.py. 

Inputs:
    company = r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt'
    sic = r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt'
    emp = r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt'
    sales = r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt'

Output:
    classification_inputYYYYMMDD.txt (tab separated)
        cols: ['DunsNumber','DunsYear','Year','Company','TradeName','SIC', 'Emp','Sales']
        n = 694,019,684
"""
#%%

import os
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import pandas as pd
import time
import nets_functions as nf
from datetime import datetime
import json

#%% LOAD CONFIG

# load in json config. this has all aux categories and their conditions.
with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230329.json', 'r') as f:
    config = json.load(f)

#%% READ IN FILES

# FULL FILES
# n = 87564680
# chunksize = 6000000

# company = r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Company.txt'
# sic = r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_SIC.txt'
# emp = r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Emp.txt'
# sales = r'Z:\UHC_Data\NETS_UHC\NETS2022\Raw Data\ASCII\NETS2022_Sales.txt'

# SAMPLE FILES
n = 1000
chunksize = 100

company = r'D:\NETS\NETS_2022\ProcessedData\Samples\company_sample.txt'
sic = r'D:\NETS\NETS_2022\ProcessedData\Samples\sic_sample.txt'
emp = r'D:\NETS\NETS_2022\ProcessedData\Samples\emp_sample.txt'
sales = r'D:\NETS\NETS_2022\ProcessedData\Samples\sales_sample.txt'


company_reader = pd.read_csv(company, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, 
                             chunksize=chunksize, 
                             usecols=['DunsNumber','Company','TradeName'])
sic_reader = pd.read_csv(sic, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                "SIC90",
                                                                                                                                                                "SIC91",
                                                                                                                                                                "SIC92",
                                                                                                                                                                "SIC93",
                                                                                                                                                                "SIC94",
                                                                                                                                                                "SIC95",
                                                                                                                                                                "SIC96",
                                                                                                                                                                "SIC97",
                                                                                                                                                                "SIC98",
                                                                                                                                                                "SIC99",
                                                                                                                                                                "SIC00",
                                                                                                                                                                "SIC01",
                                                                                                                                                                "SIC02",
                                                                                                                                                                "SIC03",
                                                                                                                                                                "SIC04",
                                                                                                                                                                "SIC05",
                                                                                                                                                                "SIC06",
                                                                                                                                                                "SIC07",
                                                                                                                                                                "SIC08",
                                                                                                                                                                "SIC09",
                                                                                                                                                                "SIC10",
                                                                                                                                                                "SIC11",
                                                                                                                                                                "SIC12",
                                                                                                                                                                "SIC13",
                                                                                                                                                                "SIC14",
                                                                                                                                                                "SIC15",
                                                                                                                                                                "SIC16",
                                                                                                                                                                "SIC17",
                                                                                                                                                                "SIC18",
                                                                                                                                                                "SIC19",
                                                                                                                                                                "SIC20",
                                                                                                                                                                "SIC21",
                                                                                                                                                                "SIC22"])

emp_reader = pd.read_csv(emp, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                "Emp90",
                                                                                                                                                                "Emp91",
                                                                                                                                                                "Emp92",
                                                                                                                                                                "Emp93",
                                                                                                                                                                "Emp94",
                                                                                                                                                                "Emp95",
                                                                                                                                                                "Emp96",
                                                                                                                                                                "Emp97",
                                                                                                                                                                "Emp98",
                                                                                                                                                                "Emp99",
                                                                                                                                                                "Emp00",
                                                                                                                                                                "Emp01",
                                                                                                                                                                "Emp02",
                                                                                                                                                                "Emp03",
                                                                                                                                                                "Emp04",
                                                                                                                                                                "Emp05",
                                                                                                                                                                "Emp06",
                                                                                                                                                                "Emp07",
                                                                                                                                                                "Emp08",
                                                                                                                                                                "Emp09",
                                                                                                                                                                "Emp10",
                                                                                                                                                                "Emp11",
                                                                                                                                                                "Emp12",
                                                                                                                                                                "Emp13",
                                                                                                                                                                "Emp14",
                                                                                                                                                                "Emp15",
                                                                                                                                                                "Emp16",
                                                                                                                                                                "Emp17",
                                                                                                                                                                "Emp18",
                                                                                                                                                                "Emp19",
                                                                                                                                                                "Emp20",
                                                                                                                                                                "Emp21",
                                                                                                                                                                "Emp22"])

sales_reader = pd.read_csv(sales, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                    "Sales90",
                                                                                                                                                                    "Sales91",
                                                                                                                                                                    "Sales92",
                                                                                                                                                                    "Sales93",
                                                                                                                                                                    "Sales94",
                                                                                                                                                                    "Sales95",
                                                                                                                                                                    "Sales96",
                                                                                                                                                                    "Sales97",
                                                                                                                                                                    "Sales98",
                                                                                                                                                                    "Sales99",
                                                                                                                                                                    "Sales00",
                                                                                                                                                                    "Sales01",
                                                                                                                                                                    "Sales02",
                                                                                                                                                                    "Sales03",
                                                                                                                                                                    "Sales04",
                                                                                                                                                                    "Sales05",
                                                                                                                                                                    "Sales06",
                                                                                                                                                                    "Sales07",
                                                                                                                                                                    "Sales08",
                                                                                                                                                                    "Sales09",
                                                                                                                                                                    "Sales10",
                                                                                                                                                                    "Sales11",
                                                                                                                                                                    "Sales12",
                                                                                                                                                                    "Sales13",
                                                                                                                                                                    "Sales14",
                                                                                                                                                                    "Sales15",
                                                                                                                                                                    "Sales16",
                                                                                                                                                                    "Sales17",
                                                                                                                                                                    "Sales18",
                                                                                                                                                                    "Sales19",
                                                                                                                                                                    "Sales20",
                                                                                                                                                                    "Sales21",
                                                                                                                                                                    "Sales22"])

#%% CREATE CLASSIFICATION INPUT

readers = zip(sic_reader, emp_reader, sales_reader, company_reader)
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk) in enumerate(readers):
    header = (c==0)
    classification_wide = nf.merge_sic_emp_sales(sic_chunk, emp_chunk, sales_chunk, company_chunk)
    classification_long = nf.normal_to_long(classification_wide, header)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)


#%% data check

class_input = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData/classification_inputYYYYMMDD.txt', sep='\t', dtype={'DunsNumber':str})
