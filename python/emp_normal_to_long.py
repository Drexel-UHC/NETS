# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 11:53:44 2022

@author: stf45
"""

import pandas as pd
import time



######## EMP #######################
'''
This script converts the NETS2019_Emp file to long format, resulting in a file 
with dunsnumber/year records and "Emp" and "EmpC" columns. The output will be 
merged with the sic_long and sales_long datasets in order to classify 
dunsnumber/years into three letter coded categories.

Input: 
    NETS2019_Emp.txt (original NETS file) n=71498225
Output: 
    emp_long.txt n=564824373 

'''

#%%

def emp_normal_to_long(empchunk, header):
    
    
    empchunk.drop(columns=["EmpHere",
                            "EmpHereC",
                            "SizeCat",
                            "EmpC90",
                            "EmpC91",
                            "EmpC92",
                            "EmpC93",
                            "EmpC94",
                            "EmpC95",
                            "EmpC96",
                            "EmpC97",
                            "EmpC98",
                            "EmpC99",
                            "EmpC00",
                            "EmpC01",
                            "EmpC02",
                            "EmpC03",
                            "EmpC04",
                            "EmpC05",
                            "EmpC06",
                            "EmpC07",
                            "EmpC08",
                            "EmpC09",
                            "EmpC10",
                            "EmpC11",
                            "EmpC12",
                            "EmpC13",
                            "EmpC14",
                            "EmpC15",
                            "EmpC16",
                            "EmpC17",
                            "EmpC18",
                            "EmpC19"], inplace=True)
    
    # swing EmpYY column to long, add key column "Year", value column "Emp"
    long_empchunk = pd.wide_to_long(empchunk, stubnames="Emp", i="DunsNumber", j= "Year", suffix='\d+')
    long_empchunk = long_empchunk.reset_index()
    
    # create list with four digit years 
    YearFull = []
    for num in long_empchunk["Year"]:
        if len(str(num)) == 1:
            YearFull.append("200" + str(num))
        elif num < 90: 
            YearFull.append("20" + str(num))        
        else:
            YearFull.append("19" + str(num))
        
    
    # add four digit years to dataframe, add new column with dunsnumber, drop 2 digit year column
    long_empchunk.drop(columns=['Year'], inplace=True)
    long_empchunk["YearFull"] = YearFull
    long_empchunk["DunsYear"] = long_empchunk["DunsNumber"] + "_" + long_empchunk["YearFull"]
    
    
    # removing rows with null values in "Emp" column, set Emp dtype
    long_empchunk.dropna(how='any', inplace=True)
    long_empchunk = long_empchunk.astype({'Emp': int})
    
    ## remove index here
    long_empchunk.to_csv("scratch/emp_long.txt", sep="\t", header=header, mode='a', index=False)
    
#%%

## sample file
csv = r"C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt"
chunksize=100
n=1000
reader = pd.read_csv(csv, sep = '\t', dtype={"DunsNumber": str}, chunksize=chunksize, header=0)

## full file
# csv = r"D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt"
# chunksize=1500000
# n=71498225
# reader = pd.read_csv(csv, sep = '\t', dtype={"DunsNumber": str}, chunksize=chunksize, header=0)


for c, chunk in enumerate(reader):
    tic = time.perf_counter()
    header = (c==0)
    emp_normal_to_long(chunk, header)
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))
    # if c==3:
    #     break


#%%    

# data check sample

long_emp_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch\emp_long.txt", sep = '\t', 
                        dtype={"DunsNumber": str}, 
                        header=0, chunksize=5000000)
                        

sample = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch\emp_long.txt", sep = '\t', dtype={"DunsNumber": str}, header=0)
emplong = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\nets_intermediate\emp_long.txt", sep = '\t', dtype={"DunsNumber": str}, header=0, nrows=1000)


lens=[]
for i, c in enumerate(long_emp_reader):
    tic = time.perf_counter()
    lens.append(len(c))
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes'.format(i+1, round(t/60,2)))

print(sum(lens))



df = pd.concat((x.query("DunsNumber == '001005383'") for x in long_emp1), ignore_index=True)

