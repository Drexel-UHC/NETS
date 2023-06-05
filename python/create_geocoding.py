# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This script creates the dataset necessary to geocode all of the categorized NETS addresses.
The geocoding_final dataset will only include locations with DunsNumbers that have 
been categorized using the Business Data Categorization process.

Inputs:
    NETS2019_Company.txt - original NETS file
    NETS2019_Move.txt - original NETS file
        
Outputs:
    geocoding_1_YYYYMMDD.txt - intermediate file containing combined Move and Company datasets,
        only including necessary columns
    geocoding_finalYYYYMMDD.txt - final dataset to be used for geocoding all NETS addresses
    
Runtime: approx 45 mins

"""

#%%
import os
os.chdir(r'C:\Users\stf45\Documents\NETS\Processing\python')
import pandas as pd
import time
import sys
import numpy as np
import nets_functions as nf
from datetime import datetime


#%% LOAD LIST OF RELEVANT DUNSNUMBERS

dunsdf = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\classified_long20230526.txt', sep='\t', usecols=(['DunsYear']))
dunsset = set(dunsdf['DunsYear'].str[:9])
del dunsdf

# takes about 4 mins

#%% ADD COMPANY FILE COLUMNS TO NEW GEOCODING_1 FILE

'''
-load in NETS2019_Company file with selected columns (see usecols)
-create new column "GcLastYear"; fill with 3000 as a filler for most recent year
-add "Gc" to start of column names to show that they are associated with the
geocoding dataset
-output to new csv "geocoding_1.txt"
'''

# for sample
# company = r"C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt"
# chunksize=100
# n=1000

# for full file
company = r"D:\NETS\NETS_2019\RawData\NETS2019_Company.txt"
chunksize=10000000
n=71498225

company_reader = pd.read_csv(company, sep = '\t', dtype=object, header=0,
                                   usecols=['DunsNumber',
                                            'Address',
                                            'City',
                                            'State',
                                            'ZipCode',
                                            'ZIP4'],
                                   chunksize=chunksize,
                                   encoding_errors='replace'
                                   )

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

for c,chunk in enumerate(company_reader):
    header = (c==0)
    chunk = chunk.loc[chunk['DunsNumber'].isin(dunsset)]
    chunk['GcLastYear'] = 3000 
    chunk['ZIP4'] = chunk['ZIP4'].replace('0000', np.nan)
    chunk = chunk.rename(columns={"Address":"GcAddress", "City":"GcCity", "State":"GcState", "ZipCode":"GcZIP", "ZIP4": "GcZIP4"})
    chunk.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1_YYYYMMDD.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

del chunk

# takes about 13mins

#%% APPEND MOVE FILE COLUMNS TO GEOCODING_1 FILE

'''
-load in NETS2019_Move file
-rename columns and arrange to match geocoding_1 csv
-append to geocoding_1 csv
'''

tic = time.perf_counter()

# for sample
# move = r"C:\Users\stf45\Documents\NETS\Processing\samples\move_sample.txt"

# for full 
move = r"D:\NETS\NETS_2019\RawData\NETS2019_Move.txt"

move_df = pd.read_csv(move, sep = '\t', dtype=object, header=0, 
                          usecols=['DunsNumber',
                                    'MoveYear',
                                    'OriginAddress',
                                    'OriginCity',
                                    'OriginState',
                                    'OriginZIP'],
                          encoding_errors='replace'
                          )

move_df = move_df.loc[move_df['DunsNumber'].isin(dunsset)]
move_df = move_df.rename(columns={"MoveYear": "GcLastYear", "OriginAddress":"GcAddress", "OriginCity":"GcCity", "OriginState":"GcState", "OriginZIP":"GcZIP"})
move_df['GcZIP4'] = np.nan
move_df = move_df[['DunsNumber',
                    'GcAddress',
                    'GcCity',
                    'GcState',
                    'GcZIP',
                    'GcZIP4',
                    'GcLastYear'
                    ]]

move_df.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1_YYYYMMDD.txt", sep="\t", header=False, mode='a', index=False)

del move_df
# del dunsset
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))

# takes about 1.5mins

#%% MERGE FIRST_LAST TO GEOCODING_1 AND WRANGLE

'''
This cell applies the nf.geocoding_wrangle function on the two halves of the geocoding_1
file. Applying the first half to the function will result in a new csv "geocoding_final".
Applying the second half to the function will result in the second half's data
being appended to the geocoding_final file. The geocoding_final file contains the
columns necessary for geocoding the NETS dataset. Dataframes are deleted when no longer
necessary for memory purposes.
'''

# read in ['DunsNumber','FirstYear','LastYear'] columns from NETS2019_Misc.txt and add +1 to all rows in FirstYear col
#(There are no SIC data for the first year of all records, so the given first year should not
#be included)

# # for sample
# misc = r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt'

# # for full
misc = r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\RawData\NETS2019_Misc\NETS2019_Misc.txt"

first_last_df = pd.read_csv(misc, sep = '\t', header=0, usecols=['DunsNumber','FirstYear','LastYear'],encoding_errors='replace')
first_last_df['FirstYear'] += 1

schema = {'DunsNumber': int,
            'GcAddress': str,
            'GcCity': str,
            'GcState': str,
            'GcZIP': str,
            'GcZIP4': str,
            'GcLastYear': int}


# takes about 11mins
tic = time.perf_counter()
geocoding_1_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1_YYYYMMDD.txt", sep="\t", dtype=schema, header=0, chunksize=10000000)
geo_first_half = pd.concat((x.query("DunsNumber <= 100000000") for x in geocoding_1_reader), ignore_index=True)
print("{} gigabytes".format(sys.getsizeof(geo_first_half)/1024**3))
nf.geocoding_wrangle(geo_first_half, first_last_df, True)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
        
del geo_first_half

# takes about 12mins
tic = time.perf_counter()
geocoding_1_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1_YYYYMMDD.txt", sep="\t", dtype=schema, header=0, chunksize=10000000)
geo_second_half = pd.concat((x.query("DunsNumber > 100000000") for x in geocoding_1_reader), ignore_index=True)
print("{} gigabytes".format(sys.getsizeof(geo_second_half)/1024**3))
nf.geocoding_wrangle(geo_second_half, first_last_df, False)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
del geo_second_half

del first_last_df

# takes about 26mins

#%% DATA CHECK

geocoding_1 = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1_YYYYMMDD.txt", dtype={'DunsNumber': str, 'GcZIP4':str}, sep = '\t', header=0)
print(geocoding_1.columns)

geocoding_final = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_finalYYYYMMDD.txt", dtype={'DunsNumber': str, 'GcZIP': str, 'GcZIP4': str}, sep = '\t', header=0)

