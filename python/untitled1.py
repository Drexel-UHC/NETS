# -*- coding: utf-8 -*-
"""
Created on Mon May  2 10:52:14 2022

@author: stf45
"""

import pandas as pd
import time


#%%

def merge_sic_emp_sales(sic_chunk, emp_chunk, sales_chunk, company_chunk):
    
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    classification_wide = pd.merge(emp_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%%

def normal_to_long(chunk, header):
    
    # swing SalesYY, EmpYY, SICYY columns to long, add key column "Year", value columns "Sales", "Emp", "SIC"
    long_chunk = pd.wide_to_long(chunk, stubnames=["Sales", "Emp", "SIC"], i="DunsNumber", j= "Year", suffix='\d+')
    long_chunk['Sales'].fillna(0, inplace=True)
    long_chunk = long_chunk.astype(str)
    long_chunk = long_chunk.reset_index()
    long_chunk.dropna(subset=['SIC'], inplace=True)
    
    # # create list with four digit years 
    # YearFull = []
    # for num in long_chunk["Year"]:
    #     if len(str(num)) == 1:
    #         YearFull.append("200" + str(num))
    #     elif num < 90: 
    #         YearFull.append("20" + str(num))        
    #     else:
    #         YearFull.append("19" + str(num))
        
    
    # # for each dataframe, add four digit years to dataframe, add new column with dunsnumber + year
    # long_chunk["YearFull"] = YearFull
    # long_chunk["DunsYear"] = long_chunk["DunsNumber"] + "_" + long_chunk["YearFull"]
    
    # # merge dataframes keeping only necessary columns and removing duplicates; removing rows with null values in "Sales" column
    # long_chunk.drop(columns=['Year'], inplace=True)
    # long_chunk = long_chunk.astype({'Sales': int, 'Emp': int})
    # long_chunk = long_chunk[['DunsNumber','DunsYear','YearFull','Company','TradeName','SIC', 'Emp','Sales']]
    # ## remove index here
    # long_chunk.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classification.txt", sep="\t", header=header, mode='a', index=False)
    return long_chunk

#%%


n = 1000
chunksize = 1000
company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, usecols=['DunsNumber','Company','TradeName'])
sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, usecols=["DunsNumber",
                                                                                                                                                                "SIC90",
                                                                                                                                                                "SIC91",
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

emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, usecols=["DunsNumber",
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

sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, usecols=["DunsNumber",
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
header = True

classification_wide = merge_sic_emp_sales(sic_reader, emp_reader, sales_reader, company_reader)
classification_long = normal_to_long(classification_wide, header)

#%%
check = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classification.txt', sep='\t', dtype = object, nrows=7681)
check2 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt', sep='\t', dtype = object, nrows=7681)

check.sort_values(by=['DunsYear'], inplace=True)
check2.sort_values(by=['DunsYear'], inplace=True)
df=pd.concat([check,check2]).drop_duplicates(keep=False)
