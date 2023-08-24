# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 09:49:01 2023

@author: stf45
"""

import pandas as pd
import sqlalchemy
import urllib
from datetime import datetime
import time

#%% LOAD PARTIAL FILE

# fill in drexel user ID here
uid = "stf45@drexel.edu"
params = urllib.parse.quote_plus(f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:netsdata.database.windows.net,1433;Database=NETS;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), echo=False)


con = engine.connect()


# Open engine in context manager
# Perform query and save results to DataFrame: df. fetchmany() grabs a requested number of rows.
# df.columns = rs.keys() sets df columns to column names from db table 

rs = con.execute("SELECT DunsYear FROM BusinessInfo")
df = pd.DataFrame(rs.fetchall())
df.columns = rs.keys()

#%% LOAD FULL FILE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()
lens = []

n = 254297507
chunksize = 20000000
fullfile = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\BusinessInfoDB20230803.txt', sep='\t', chunksize=chunksize)


for i, chunk in enumerate(fullfile):
    header = (i==0)
    lens.append(len(chunk)) # to find len of df
    loadfile = chunk.loc[~chunk['DunsYear'].isin(df['DunsYear'])]
    loadfile.to_csv(r'D:\NETS\NETS_2019\ProcessedData\BusinessInfoRemaining20230814.txt', sep='\t', index=False, mode='a', header=header)   
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(i+1, round(t/60, 2), n/chunksize-(i+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)    
print(f'number of rows: {sum(lens)}')

con.close()

#%% CHECK FILE


