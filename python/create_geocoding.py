# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45

This script creates the dataset necessary to geocode all of the NETS addresses.

Inputs:
    NETS2019_Company.txt - original NETS file
    NETS2019_Move.txt - original NETS file
    first_last.txt - derived from NETS2019_SIC dataset, containing dunsnumber and
        separate 'FirstYear' and 'LastYear' columns depicting the first and
        last years that a dunsnumber existed.
        
Outputs:
    geocoding_1.txt - intermediate file containing combined Move and Company datasets,
        only including necessary columns
    geocoding_2.txt - final dataset to be used for geocoding all NETS addresses
    
Runtime: approx 1.25 hours

"""

#%%
import os
os.chdir(r'C:\Users\stf45\Documents\NETS\Processing\python')
import pandas as pd
import time
import sys
import numpy as np
import nets_functions as nf

#%% ADD COMPANY FILE COLUMNS TO NEW GEOCODING_1 FILE

'''
-load in NETS2019_Company file with selected columns (see usecols)
-create new column "GcLastYear"; fill with 3000 as a filler for most recent year
-add "Gc" to start of column names to show that they are associated with the
geocoding dataset
-output to new csv "geocoding_1.txt"
'''


# for sample
company = r"C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt"
comp_chunksize=100
comp_n=1000

# for full file
# company = r"D:\NETS\NETS_2019\RawData\NETS2019_Company.txt"
# comp_chunksize=10000000
# comp_n=71498225

company_reader = pd.read_csv(company, sep = '\t', dtype=object, header=0,
                                   usecols=['DunsNumber',
                                            'Address',
                                            'City',
                                            'State',
                                            'ZipCode',
                                            'ZIP4'],
                                   chunksize=comp_chunksize,
                                   encoding_errors='replace'
                                   )

for i,chunk in enumerate(company_reader):
    header = (i==0)
    tic = time.perf_counter()
    chunk['GcLastYear'] = 3000 
    chunk['ZIP4'].replace('0000', np.nan, inplace=True)
    chunk.rename(columns = {"Address":"GcAddress", "City":"GcCity", "State":"GcState", "ZipCode":"GcZIP", "ZIP4": "GcZIP4"}, inplace=True)
    chunk.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes! {} chunks to go'.format(i+1, round(t/60, 2), comp_n/comp_chunksize-(i+1)))
    # if i==1:
    #     break

del chunk

#%% APPEND MOVE FILE COLUMNS TO GEOCODING_1 FILE

'''
-load in NETS2019_Move file
-rename columns and arrange to match geocoding_1 csv
-append to geocoding_1 csv
'''

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

move_df.rename(columns = {"MoveYear": "GcLastYear", "OriginAddress":"GcAddress", "OriginCity":"GcCity", "OriginState":"GcState", "OriginZIP":"GcZIP"}, inplace=True)
move_df['GcZIP4'] = np.nan
move_df = move_df[['DunsNumber',
                    'GcAddress',
                    'GcCity',
                    'GcState',
                    'GcZIP',
                    'GcZIP4',
                    'GcLastYear'
                    ]]

move_df.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", sep="\t", header=False, mode='a', index=False)

del move_df

#%% MERGE FIRST_LAST TO GEOCODING_1 AND WRANGLE

'''
This cell applies the previous cell's function on the two halves of the geocoding_1
file. Applying the first half to the function will result in a new csv "geocoding_2".
Applying the second half to the function will result in the second half's data
being appended to the geocoding_2 file. The geocoding_2 file contains the minimum
columns necessary for geocoding the NETS dataset. Dataframes are deleted when no longer
necessary for memory purposes.
'''
# for sample
# first_last = r"C:\Users\stf45\Documents\NETS\Processing\samples\first_last_sample.txt"

# for full
first_last = r"C:\Users\stf45\Documents\NETS\Processing\scratch\first_last.txt"

first_last_df = pd.read_csv(first_last, sep = '\t', header=0)

schema = {'DunsNumber': int,
            'GcAddress': str,
            'GcCity': str,
            'GcState': str,
            'GcZIP': str,
            'GcZIP4': str,
            'GcLastYear': int}


# takes about 30mins
geocoding_1_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", sep="\t", dtype=schema, header=0, chunksize=10000000)
geo_first_half = pd.concat((x.query("DunsNumber <= 100000000") for x in geocoding_1_reader), ignore_index=True)
print("{} gigabytes".format(sys.getsizeof(geo_first_half)/1024**3))
tic = time.perf_counter()
nf.geocoding_wrangle(geo_first_half, first_last_df, True)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
        
del geo_first_half

# takes about 30mins
geocoding_1_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", sep="\t", dtype=schema, header=0, chunksize=10000000)
geo_second_half = pd.concat((x.query("DunsNumber > 100000000") for x in geocoding_1_reader), ignore_index=True)
print("{} gigabytes".format(sys.getsizeof(geo_second_half)/1024**3))
tic = time.perf_counter()
nf.geocoding_wrangle(geo_second_half, first_last_df, False)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
del geo_second_half

del first_last_df



#%% DATA CHECK

geocoding_1 = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", dtype={'DunsNumber': str, 'GcZIP4':str}, sep = '\t', header=0, nrows=10000)

geocoding_2 = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2.txt", dtype={'DunsNumber': str, 'GcZIP': str, 'GcZIP4': str}, sep = '\t', header=0, nrows=10000)

first_last = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\nets_intermediate/first_last.txt", dtype={'DunsNumber': str}, sep = '\t', header=0)

geocoding_2.GcFirstYear.value_counts()

