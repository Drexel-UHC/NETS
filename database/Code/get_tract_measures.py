# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 16:36:15 2023

@author: stf45
"""


import sqlalchemy 
from sqlalchemy import func, create_engine
import pandas as pd
import urllib

## prod
# fill in drexel user ID here
uid = "stf45@drexel.edu"
params = urllib.parse.quote_plus(f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:uhcdata.database.windows.net,1433;Database=NETSSample;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), echo=False)
# Open engine connection: con
con = engine.connect()

# Open engine in context manager
# Perform query and save results to DataFrame: df. fetchmany() grabs a requested number of rows.
# df.columns = rs.keys() sets df columns to column names from db table 
with engine.connect() as con:
    rs = con.execute("SELECT Col1, Col2 FROM TableName")
    df = pd.DataFrame(rs.fetchmany(size=3))
    df.columns = rs.keys()

# Print the length of the DataFrame df
print(len(df))

# Print the head of the DataFrame df
print(df.head())


