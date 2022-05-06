# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 11:53:44 2022

@author: stf45
"""

import pandas as pd
import time
import numpy as np

######## SALES #######################

'''

sales_long n / sic_long_filter n is proportionately equal to 
sales_long_sample n / sic_long_filter_sample n


Input:
    NETS2019_Sales.txt
Output:
    sales_long.txt n=560177509
'''
#%%

def sales_normal_to_long(chunk, header):
    
    # drop unnecessary columns
    chunk.drop(columns=['SalesHere',
                        'SalesHereC',
                        'SalesGrowth',
                        'SalesGrowthPeer',
                        "SalesC90",
                        "SalesC91",
                        "SalesC92",
                        "SalesC93",
                        "SalesC94",
                        "SalesC95",
                        "SalesC96",
                        "SalesC97",
                        "SalesC98",
                        "SalesC99",
                        "SalesC00",
                        "SalesC01",
                        "SalesC02",
                        "SalesC03",
                        "SalesC04",
                        "SalesC05",
                        "SalesC06",
                        "SalesC07",
                        "SalesC08",
                        "SalesC09",
                        "SalesC10",
                        "SalesC11",
                        "SalesC12",
                        "SalesC13",
                        "SalesC14",
                        "SalesC15",
                        "SalesC16",
                        "SalesC17",
                        "SalesC18",
                        "SalesC19"], inplace=True)
    
    chunk.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    
    # swing SalesYY column to long, add key column "Year", value column "Sales"
    long_chunk = pd.wide_to_long(chunk, stubnames="Sales", i="DunsNumber", j= "Year", suffix='\d+')
    long_chunk = long_chunk.reset_index()
    
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
    long_chunk["DunsYear"] = long_chunk["DunsNumber"] + "_" + long_chunk["YearFull"]
    
    # merge dataframes keeping only necessary columns and removing duplicates; removing rows with null values in "Sales" column
    long_chunk.drop(columns=['Year'], inplace=True)
    long_chunk.dropna(how='any', inplace=True)
    long_chunk = long_chunk.astype({'Sales': int})
    
    ## remove index here
    long_chunk.to_csv("scratch/sales_long.txt", sep="\t", header=header, mode='a', index=False)
    
#%%

## full file
# csv = r"D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt"
# chunksize=3000000
# n=71498225
# reader = pd.read_csv(csv, sep = '\t', dtype={"DunsNumber": str}, chunksize=chunksize, header=0, encoding_errors='replace')

## sample file
csv = r"C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt"
chunksize=100
n=1000
reader = pd.read_csv(csv, sep = '\t', dtype={"DunsNumber": str}, chunksize=chunksize, header=0)


for c, chunk in enumerate(reader):
    tic = time.perf_counter()
    header = (c==0)
    sales_normal_to_long(chunk, header)
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

#%%    

# data check sample

long_sales = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch\sales_long.txt", sep = '\t', header=0, dtype={"DunsNumber": str}, nrows=1000)

df = pd.concat((x.query("DunsNumber <= 20000000") for x in long_sales), ignore_index=True)

val_check = df.loc[df["DunsNumber"] == 1005383]





