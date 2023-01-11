# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 12:32:24 2022

@author: stf45

55510200
55510201
55510202
55510203
59999913

get frequencies of these!^^
"""




import pandas as pd
import time
#%%

n=71498225
chunksize=30000000
sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str, 'SIC19':str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC19"])

#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

time_list = []
siclist = ['59999913']

for c, sic_chunk in enumerate(sic_reader):
    tic = time.perf_counter()
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['SIC19'].isin(siclist)]
    sic_chunk.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/sic_check20221201.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)     

#%% DATA CHECK

check = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\sic_check20221201.txt', sep='\t')

freqs = check['SIC19'].value_counts().reset_index()
