# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 07:48:14 2023

@author: stf45


This script checks to see what the first year that sic 59939905 (cannabis stores)
appears in the NETS 2019 dataset.

Inputs: D:\NETS\NETS_2019\RawData\
    NETS2019_SIC.txt
    NETS2019_Company.txt
    NETS2019_Emp.txt
    NETS2019_Misc.txt
    NETS2019_Sales.txt

Outputs:
    nets_cannabis_store_2019_20230309.xlsx
    
runtime: 63 mins

"""
#%%
import pandas as pd
import time
from datetime import datetime

#%%

# # SAMPLE FILES
# n = 1000
# chunksize = 100
# file = r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt'


# FULL FILES:           
n = 71498225
chunksize = 10000000
file = r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt'

sic_reader = pd.read_csv(file, sep = '\t', dtype=str,  
                         header=0, chunksize=chunksize, encoding_errors='replace')

                                                                                                                                   
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

canna_firstyear = pd.DataFrame()

for c, sic_chunk in enumerate(sic_reader):
    header = (c==0)
    # swing SalesYY, EmpYY, SICYY columns to long, add key column "Year", value columns "Sales", "Emp", "SIC"
    sic_chunk = sic_chunk.drop(columns=['SICChange', 'SIC8_6', 'Industry', 'SIC2', 'SIC3', 'SIC4', 'SIC6', 'SIC8',
           'IndustryGroup', 'SIC8_2', 'SIC8_3', 'SIC8_5', 'SIC8_4'])
    long_chunk = pd.wide_to_long(sic_chunk, stubnames=["SIC"], i="DunsNumber", j= "Year", suffix='\d+')
    long_chunk = long_chunk.reset_index()
    long_chunk = long_chunk.dropna(subset=['SIC'])

    
    # create list with four digit years 
    YearFull = []
    for num in long_chunk["Year"]:
        if len(str(num)) == 1:
            YearFull.append("200" + str(num))
        elif num < 90: 
            YearFull.append("20" + str(num))        
        else:
            YearFull.append("19" + str(num))
        
    
    # for each dataframe, add four digit years to dataframe, add new column with dunsnumber + year
    long_chunk["YearFull"] = YearFull
    long_chunk = long_chunk.drop(columns=['Year'])
    canna = long_chunk.loc[long_chunk['SIC'] == '59939905']  
    canna_firstyear = pd.concat([canna_firstyear, canna])
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)     

#%% READ IN CSV

cannafirst = pd.read_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/cannabis_firstyear_20230309.txt", sep = '\t', dtype={"DunsNumber": str},  header=0)

#%%

uniques = canna_firstyear['YearFull'].value_counts().reset_index().rename(columns={'index':'Year', 'YearFull':'Count'})

#%% WRITE TABLE TO EXCEL

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\nets_cannabis_store_2019_20230309.xlsx') as writer:
    uniques.to_excel(writer, "records w sic 59939905 (2019)", index=False)



