# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This script takes the full company, sic, emp, and sales files and pivots them
into a long file to be used as the input for classify.py. 

Inputs:
    company = r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt'
    sic = r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt'
    emp = r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt'
    sales = r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt'

Output:
    classification.txt (tab separated)
        cols: ['DunsNumber','DunsYear','YearFull','Company','TradeName','SIC', 'Emp','Sales']
        n = 564,824,373
"""
#%%

import os
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import pandas as pd
import time
import nets_functions as nf
from datetime import datetime

#%% READ IN FILES

# FULL FILES
n = 71498225
chunksize = 6000000

company = r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt'
sic = r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt'
emp = r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt'
sales = r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt'

# # SAMPLE FILES
# n = 1000
# chunksize = 100

# company = r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt'
# sic = r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt'
# emp = r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt'
# sales = r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt'


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
                                                                                                                                                                "SIC19"])

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
                                                                                                                                                                "Emp19"])

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
                                                                                                                                                                    "Sales19"])

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

#%%

classification = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classification.txt', sep='\t', header=0, usecols=['DunsYear'],
                              chunksize=100000000)
                             
lenlist = []

for x in classification:
    lenlist.append(len(x))
    print(len(x))      


print(sum(lenlist))
