# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 09:45:07 2022

@author: stf45
"""
#%%
from sqlalchemy import Table, MetaData, create_engine, select, and_, or_
import urllib
import pandas as pd

#%%
# connect to NETSSAMPLE db
uid = 'stf45@drexel.edu'
params = urllib.parse.quote_plus(f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:uhcdata.database.windows.net,1433;Database=NETSSample;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), echo=False,)
con = engine.connect()

#%% GET DB INFO AND 'GeocodingInputSample' TABLE INFO

# get all table names in db
engine.table_names()

# get metadata for ClassifiedLongSample table
metadata = MetaData()
GeocodingInputSample = Table('GeocodingInputSample', metadata, autoload=True, autoload_with=engine)

# print table columns
print(GeocodingInputSample.columns.keys())

# print full table metadata, use replace to add newlines to make more readable
print(repr(GeocodingInputSample).replace('Column','\nColumn'))

#%% SELECT ALL COLS FROM 'GeocodingInputSample' WHERE STATE == 'NY'

# select all from GeocodingInputSample and filter for NY records
stmt = select([GeocodingInputSample])
stmt = stmt.where(GeocodingInputSample.columns.State == 'NY')

# same as two lines above but in raw sql
stmt = "SELECT * FROM GeocodingInputSample WHERE State = 'NY'"

# Execute the statement and fetch the results: results
results = con.execute(stmt).fetchall()

# add results to df
df = pd.DataFrame(results)

#%% SELECT WITH AND/OR CONDITIONS

# select all from GeocodingInputSample
stmt2 = select([GeocodingInputSample])

stmt2 = stmt2.where(and_(GeocodingInputSample.columns.State == 'NY',
                        GeocodingInputSample.columns.FirstYear >= 2000,
                        GeocodingInputSample.columns.LastYear <2009),
                   and_(or_(GeocodingInputSample.columns.Zip == '10453',
                            GeocodingInputSample.columns.Zip == '10469')))

## same as five lines above but in raw sql
# stmt2 = """
# SELECT * FROM GeocodingInputSample 
#     WHERE State =='NY'
#     AND FirstYear >= 2000
#     AND LastYear < 2009
#     AND Zip == '10453' OR Zip == '10469'
# """


# Execute the statement and fetch the results: results
results2 = con.execute(stmt2).fetchall()

# add results to df
df2 = pd.DataFrame(results2)

#%% SELECT WITH IN CONDITION

# select all from GeocodingInputSample
stmt3 = select([GeocodingInputSample])
stmt3 = stmt3.where(GeocodingInputSample.columns.State.in_(['NY','NJ','PA']))

## same as two lines above but in raw sql
# stmt3 = text(
#     "SELECT * FROM GeocodingInputSample WHERE State IN ('NY', 'PA', 'NJ')"
#     )

# Execute the statement and fetch the results: results
results3 = con.execute(stmt3).fetchall()

# add results to df
df3 = pd.DataFrame(results3)

#%% SELECT WITH JOIN

GeocodedSample = Table('GeocodedSample', metadata, autoload=True, autoload_with=engine)
ClassifiedLongSample = Table('ClassifiedLongSample', metadata, autoload=True, autoload_with=engine)
DunsMoveYearSample = Table('DunsMoveYearSample', metadata, autoload=True, autoload_with=engine)

print(repr(GeocodingInputSample).replace('Column','\nColumn'))
print(repr(ClassifiedLongSample).replace('Column','\nColumn'))
print(repr(DunsMoveYearSample).replace('Column','\nColumn'))


DunsMoveYearSample.c.keys()
GeocodedSample.columns.keys()
ClassifiedLongSample.columns.keys()


stmt4 = '''
SELECT * FROM GeocodedSample geo
LEFT JOIN DunsMoveYearSample dm ON dm.DunsMove=geo.DunsMove
LEFT JOIN ClassifiedLongSample cl ON cl.DunsYear=dm.DunsYear;
'''

# Execute the statement and get the first result: result
results4 = con.execute(stmt4).fetchall()

df4 = pd.DataFrame(results4)

    
#%% always close connection when finished

con.close()
