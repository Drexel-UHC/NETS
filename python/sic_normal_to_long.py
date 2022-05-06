# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 14:08:32 2022

@author: stf45
"""

import pandas as pd
import time



######## SIC #######################

'''
This script converts the NETS2019_SIC file into long format, where each record
is a dunsnumber/year. Dunsnumber/years with no data are removed from the output.

Input: 
    NETS2019_SIC.txt: n=71498225
    (mesa_sic.txt??) 
Output: 
    sic_long_filter.txt n=564824373 
    (sic_long.txt??) 
Runtime: approx 1.2 hours

Notes: There are two options to comment/uncomment in the normal_to_long function.
The first option keeps only the columns necessary for filtering out unnecessary
SIC codes, resulting in sic_long_filter. Do I need second option?
'''

#%%

def sic_normal_to_long(chunk, header):
    # USING JUST SICYY COLUMNS TO FILTER UNNECESSARY SICS:
    chunk = chunk.drop(columns = ['SIC2', 'SIC3', 'SIC4', 'SIC6', 'SIC8', 'SIC8_2', 'SIC8_3', 'SIC8_4', 'SIC8_5', 'SIC8_6', 'SICChange', 'Industry', 'IndustryGroup'])

    # swing SICYY columns to long, add key column "Year", value column "SIC"
    long_chunk = pd.wide_to_long(chunk, stubnames="SIC", i="DunsNumber", j= "Year", suffix='\d+')
    long_chunk = long_chunk.reset_index()
    print(long_chunk.isnull().sum(axis = 0))
    long_chunk = long_chunk.dropna(how='any')

    
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
    long_chunk.drop(['Year'], axis=1, inplace=True)
    long_chunk["YearFull"] = YearFull
    long_chunk['DunsYear'] = long_chunk['DunsNumber'] + "_" + long_chunk['YearFull']
    
    # sort by dunsnumber
    long_chunk.sort_values(['DunsNumber', 'YearFull'], inplace=True)
    
    # append chunk to csv
    long_chunk.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/sic_long_filter.txt", sep="\t", header=header,  mode='a', index=False)
    
    

#%%

# for sample file
csv = r"C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt"
chunksize=1000
n=1000
reader = pd.read_csv(csv, sep = '\t', dtype={'DunsNumber':str},
                      chunksize=chunksize,
                      header=0)

# for full file

# csv = r"D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt"
# chunksize=2859929
# n=71498225
# schema = {'DunsNumber':str, 'YearFull':int, 'SIC':str}
# reader = pd.read_csv(csv, sep = '\t', dtype= object, 
#                      chunksize=chunksize, 
#                      header=0, 
#                      )


for c, chunk in enumerate(reader):
    tic = time.perf_counter()
    header = (c==0)
    sic_normal_to_long(chunk, header)
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))
    # if c==1:
    #     break


#%%    

# data check 

long_sic = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch\sic_long_filter.txt", sep = '\t', dtype=object, header=0, nrows=10000)





