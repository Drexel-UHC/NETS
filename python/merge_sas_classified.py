# -*- coding: utf-8 -*-
"""
Created on Thu May 25 11:48:56 2023

@author: stf45
"""

import pandas as pd
from datetime import datetime
import time

#%%

df1 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\cmassmerch.csv')
df2 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\conv_gas.csv')
df3 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\conv_other.csv')
df4 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\drug.csv')
df5 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\grocery.csv')
df6 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\supercenter.csv')
df7 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\gmassmerch.csv')
df8 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\warehouse.csv')

#%% MERGE INDIVIDUAL SAS CLASSIFIED DATASETS

dflist = [df1,df2,df3,df4,df5,df6,df7,df8]
lenlist=[]
dfmain = pd.DataFrame(columns=['DunsYear'])
for df in dflist:
    lenlist.append(len(df))
    dfmain = dfmain.merge(df, how="outer")
    
    print(f'length of main: {len(dfmain)}')
    print(f'length of total up to {df.columns[1]}: {sum(lenlist)}')

# replace nans with zeros and switch to int dtype
dfmain = dfmain.fillna(0)
dfmain.iloc[:,1:] = dfmain.iloc[:,1:].astype(int)

#%% DATA CHECK

# find # of DunsYears that are flagged more than once
dups = dfmain.loc[dfmain.sum(axis=1,numeric_only=True) > 1]

#check to see if any DunsYears are flagged more than twice
len(dfmain.loc[dfmain.sum(axis=1,numeric_only=True) > 2]) == 0

# see if total # of DunsYears in input is equal to output DunsYears, counting DunsYears
#twice that are flagged twice 
sum(lenlist) - (len(dups) + len(dfmain) )

dfmainhead = dfmain.head(500000)

dfmain.to_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classified_sas20230526.txt', sep='\t', index=False)


###############################################################################
## STUFF BELOW HERE WILL PROBABLY BE DELETED

#%% delete unecessary variables

del[df1,df2,df3,df4,df5,df6,df7,df8,lenlist]

#%% COMPARE DUNSYEARS FROM CLASSIFIED DATASET (SAS) AND CLASSIFIED DATASET (PYTHON)

# check to see if there are overlapping DunsYears with classified dataset (python)

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

sasdunsset = set(dfmain['DunsYear'])
pydunsset = set()
chunksize = 50000000
n = 254288461
classified_reader = pd.read_csv(r'D:\NETS\NETS_2019\ProcessedData\classified20230410.txt', chunksize=chunksize, usecols=['DunsYear'], header=0, sep = '\t')

for c,chunk in enumerate(classified_reader):
    pydunsset.update(set(chunk['DunsYear']))
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)


# take dunsyears in SAS dataset that ARE in python dataset and save as LEFT JOIN df
# take remaining dunsyears and save as CONCAT df 








#%% CHECK OVERLAPPING DUNSYEARS BETWEEN SAS AND PYTHON CLASSIFIED DATSETS
# if there is no overlap, you can just write Classified Dataset (SAS) onto the
#end of the Classfied Dataset (Python). If not, you will have to do a creative 
#outer merge in chunks


sasdunsset2 = sasdunsset.update(pydunsset)

sasdunsset2 == pydunsset
