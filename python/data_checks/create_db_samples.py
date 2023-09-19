# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 22:23:32 2023

@author: stf45
"""

import pandas as pd
import time
from datetime import datetime
#%%

company_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype=object, usecols = ['DunsNumber'],  header=0)
dunsnumbers = set(company_sample['DunsNumber'])

#%%
classifiedlong = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\ClassifiedLong20230526.txt', sep='\t')
classifiedlong = classifiedlong.loc[classifiedlong['DunsYear'].str[:9].isin(dunsnumbers)]
classifiedlong.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dbsamples\ClassifiedLongDBsample.txt', sep='\t', index=False)

#%%
dunsmovedunsyearkey = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\DunsMove_DunsYear_Key20230711.txt', sep='\t')
dunsmovedunsyearkey = dunsmovedunsyearkey.loc[dunsmovedunsyearkey['DunsYear'].str[:9].isin(dunsnumbers)]
dunsmovedunsyearkey.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dbsamples\DunsMove_DunsYear_KeyDBsample.txt', sep='\t', index=False)

#%%
addressid = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\Addresses20230608.txt', sep='\t')
addressid = addressid.loc[addressid['AddressID'].isin(dunsmovedunsyearkey['AddressID'])]
addressid.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dbsamples\AddressesDBsample.txt', sep='\t', index=False)

#%%
dunslocs = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\DunsLocations20230628.csv', dtype={'GEOID10':str})
dunslocs = dunslocs.loc[dunslocs['USER_AddressID'].isin(addressid['AddressID'])]
dunslocs = dunslocs.drop(columns=['OID_'])
dunslocs = dunslocs.rename(columns={'USER_AddressID':'AddressID'})
dunslocs.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dbsamples\DunsLocationsDBsample.txt', sep='\t', index=False)

#%%
chunksize=10000000
n = 254297507
businessinfo_reader = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\BusinessInfoDB20230710.txt', sep='\t', dtype={'DunsNumber':str, 'SIC':str}, chunksize=chunksize)

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

rownum = []
for c,x in enumerate(businessinfo_reader):
    header = (c==0)
    rownum.append(len(x))
    x = x.loc[x['DunsYear'].str[:9].isin(dunsnumbers)]
    x.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\dbsamples\BusinessInfoDBsample.txt', sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)
print(f'BusinessInfoDBYYYYMMDD has {sum(rownum)} rows')
