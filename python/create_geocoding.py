# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:41:16 2022

@author: stf45
"""

import pandas as pd
import time
import sys
import numpy as np

'''
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

'''
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

#%% APPEND MOVE FILE COLUNMS TO GEOCODING_1 FILE

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

#%% MERGE FIRST_LAST TO GEOCODING_1 AND WRANGLE FUNCTION

'''
-takes one of the geocoding dataset halves (queried in the next cell as either 
"DunsNumber <= 100000000" or "DunsNumber > 100000000"), merges it with the 
first_last dataset so each dunsnumber/address gets a first year ('FirstYear') 
and last year ('LastYear') that the dunsnumber was in business
-applies the LastYear to the GcLastYear column for all of the most recent 
dunsnumber/addresses
-creates a GcFirstYear for all dunsnumber/addresses using either FirstYear
(if it is the first address) or the previous address' GcLastYear + 1
-drop unnecessary columns and reorder
-write out to csv "geocoding_2"
'''

def geocoding_wrangle(geocoding_1_half, first_last, header):
    geocoding_2 = geocoding_1_half.merge(first_last, on='DunsNumber')
    
    # use zfill to replace leading zeros in dunsnumbers
    geocoding_2['DunsNumber'] = geocoding_2['DunsNumber'].astype(str).str.zfill(9)
    
    # create column geocoding_2["MoveNum"] that indicates 0 for most recent address, 
    #1 for second to last address, 2 for third to last address, etc
    # create column geocoding_2['DunsMove'] that concatenates 1 + movenum + dunsnumber
    movenum = []
    
    geocoding_2.sort_values(by=['DunsNumber', 'GcLastYear'], ascending=[True, False], inplace=True)
    geocoding_2['dunslag'] = geocoding_2['DunsNumber'].shift(1)
    for i, row in geocoding_2.iterrows():
        if row['DunsNumber'] != row['dunslag']:
            count=0
            movenum.append(count)
        else:
            count += 1
            movenum.append(count)
    
    geocoding_2['MoveNum'] = movenum
    geocoding_2['DunsMove'] = "1" + geocoding_2['MoveNum'].astype(str) + geocoding_2['DunsNumber']
    
    # set values in 'GcLastYear' == 3000 to value in 'LastYear', all other values equal
    # This applies the last year of a dunsnumber's existence (last year that data
    #is given) to the 'GcLastYear' for all addresses from the company dataset,
    #which represent only the most recent addresses. The GcLastYear's for the 
    #other (move) addresses are derived from move['MoveYear']
    geocoding_2['GcLastYear'] = np.where(geocoding_2['GcLastYear'] == 3000, geocoding_2['LastYear'], geocoding_2['GcLastYear']).astype(int)
    
    
    # group by dunsnumber to find lowest 
    geocoding_2['min'] = geocoding_2['GcLastYear'].groupby(geocoding_2['DunsNumber']).transform('min')
    geocoding_2['GcLastLag'] = geocoding_2['GcLastYear'].shift(-1)
    
    # make this so the last argument is GcLastLag + 1
    geocoding_2['GcFirstYear'] = np.where(geocoding_2['GcLastYear'] == geocoding_2['min'], geocoding_2['FirstYear'], geocoding_2['GcLastLag'] + 1).astype(int)
    
    geocoding_2 = geocoding_2.drop(columns=['dunslag', 'min', 'GcLastLag', 'FirstYear', 'LastYear'])
    
    geocoding_2 = geocoding_2[['DunsNumber',
                                    'DunsMove',
                                    'MoveNum',
                                    'GcFirstYear',
                                    'GcLastYear',
                                    'GcAddress',
                                    'GcCity',
                                    'GcState',
                                    'GcZIP',
                                    'GcZIP4'
                                    ]]
    
    
    geocoding_2.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2.txt", sep="\t", mode='a', header=header, index=False)

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
first_last = r"C:\Users\stf45\Documents\NETS\Processing\nets_intermediate\first_last.txt"


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
geocoding_wrangle(geo_first_half, first_last_df, True)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
del geo_first_half

# takes about 30mins
geocoding_1_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", sep="\t", dtype=schema, header=0, chunksize=10000000)
geo_second_half = pd.concat((x.query("DunsNumber > 100000000") for x in geocoding_1_reader), ignore_index=True)
print("{} gigabytes".format(sys.getsizeof(geo_second_half)/1024**3))
tic = time.perf_counter()
geocoding_wrangle(geo_second_half, first_last_df, False)
toc = time.perf_counter()
t = toc - tic
print('time: {} minutes'.format(round(t/60, 2)))
del geo_second_half

del first_last_df



#%% DATA CHECK

geocoding_1 = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", dtype={'DunsNumber': str}, sep = '\t', header=0, skiprows=71498220, nrows=10000)
geocoding_1_sample = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_1.txt", dtype={'DunsNumber': str}, sep = '\t', header=0)


geocoding_2_sample = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2.txt", dtype={'DunsNumber': str, 'GcZIP': str, 'GcZIP4': str}, sep = '\t', header=0)
geocoding_2 = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2.txt", dtype={'DunsNumber': str, 'GcZIP': str, 'GcZIP4': str}, sep = '\t', header=0)

first_last = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\nets_intermediate/first_last.txt", dtype={'DunsNumber': str}, sep = '\t', header=0)


geocoding_2.GcFirstYear.value_counts()

df = geocoding_2.query("DunsNumber == '038928987'")


geocoding_2_reader = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2.txt", chunksize=40000000, sep = '\t', header=0)

lens=[]
for i, c in enumerate(geocoding_2_reader):
    tic = time.perf_counter()
    lens.append(len(c))
    toc = time.perf_counter()
    t = toc - tic
    print('chunk {} completed in {} minutes'.format(i+1, round(t/60,2)))

print(sum(lens))

