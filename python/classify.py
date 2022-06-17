# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45
"""

import pandas as pd
import time
import json
import nets_functions as nf
import warnings 

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

#%% LOAD JSON CONFIG

with open('../config/json_config_2022_04_20_MAR.json', 'r') as f:
    config = json.load(f)
#%%


n = 1000
chunksize = 100
company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, 
                             chunksize=chunksize, 
                             usecols=['DunsNumber','Company','TradeName'])
sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
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

emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
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

sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
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

#%%
readers = zip(sic_reader, emp_reader, sales_reader, company_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    classification_wide = nf.merge_sic_emp_sales(sic_chunk, emp_chunk, sales_chunk, company_chunk)
    classification_long = nf.normal_to_long(classification_wide, header)
    nf.classify(classification_long, config, header)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)


check = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_coded_sample.txt', sep='\t', dtype = object, nrows=30000)

#%% WRITE OUT REPORT

title = "NETS Classification Report Using classify.py\n"

lines = [title, runtime]
with open('classify_report.txt', 'w') as f:
    for line in lines:
        f.write(line)
        f.write('\n')

#%% GET FREQS

coded = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\NETS_coded_sample.txt', sep='\t', header = 0)

# get sum of each record's total category count
# get unique values to show how many records were not flagged (0), flagged once (1), etc
coded['cat_counts'] = coded.iloc[:,8:].sum(axis=1)
uniques = coded['cat_counts'].value_counts()
uniques = pd.DataFrame(uniques)


# get sum of each category's total record count
catcounts = coded.iloc[:,8:].sum(axis=0)
catcounts = pd.DataFrame(catcounts, columns=['count'])

#%% WRITE EXCEL REPORT

with pd.ExcelWriter('../reports/NETS_classify_report20220509.xlsx') as writer:
    uniques.to_excel(writer, sheet_name='unique_values')
    catcounts.to_excel(writer, sheet_name='cat_counts')