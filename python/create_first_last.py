# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 09:02:47 2022

@author: stf45

This script creates a "FirstYear" column depicting a dunsnumber's first year 
in business, as well as a "LastYear" column depicting its last year in business.
It takes the long version (sorted in ascending order by dunsnumber and year) 
of the NETS2019_SIC.txt file as its input, and outputs the data to a new csv: 
"first_last.txt".

"""

#%%
import os
os.chdir(r'C:\Users\stf45\Documents\NETS\Processing\python')
import pandas as pd
import time
import nets_functions as nf

# for sample file
csv = r"C:\Users\stf45\Documents\NETS\Processing\samples/sample_sic_long_filter.txt"
chunksize = 500


# for full file
csv = r"C:\Users\stf45\Documents\NETS\Processing\samples/sample_sic_long_filter.txt"



reader = pd.read_csv(csv, sep = '\t', 
                      chunksize=chunksize,
                      usecols=['DunsNumber', 'YearFull'],
                      header=0)

#%% FIND NUMBER OF ROWS OF CHUNKED FILE

lens=[]
for i, c in enumerate(reader):
    tic = time.perf_counter()
    lens.append(len(c))
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes.'.format(i+1, round(t/60,2)))

print(sum(lens))

#%% READ IN ROWS CONDITIONALLY WITH PANDAS IN CHUNKED CSV. RUN THROUGH GENERATOR

csv = r"C:\Users\stf45\Documents\NETS\Processing\scratch/sic_long_filter.txt"
chunksize = 50000000
reader = pd.read_csv(csv, sep = '\t', 
                      chunksize=chunksize,
                      usecols=['DunsNumber', 'YearFull'],
                      header=0)

# create dataframe "chunk1" from rows where dunsnumber <= 100000000, run through generator
# delete dataframe after processing
chunk1 = pd.concat((x.query("`DunsNumber` <= 100000000") for x in reader), ignore_index=True)
nf.first_last(chunk1, True)
del chunk1

#%% REINSTANTIATE READER AND RUN ON SECOND CHUNK. RUN THROUGH GENERATOR

# reinstantiate reader
# create dataframe "chunk2" from rows where dunsnumber > 100000000, run through generator
# delete dataframe after processing

reader = pd.read_csv(csv, sep = '\t', 
                     chunksize=chunksize,
                     usecols=['DunsNumber', 'YearFull'],
                     header=0)

chunk2 = pd.concat((x.query("`DunsNumber` > 100000000") for x in reader), ignore_index=True)
nf.first_last(chunk2, False)
del chunk2

#%% DATA CHECK

first_last = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/first_last.txt", sep = '\t', dtype=object, header=0)

            
            
