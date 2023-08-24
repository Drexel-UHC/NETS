import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey
import urllib
import pyodbc
import numpy as np
from math import floor
from datetime import datetime
import time
import csv

"""f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:netsdata.database.windows.net,1433;Database=NETS;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;"
"""
#%% SET UP DB CONNECTION
## prod
# fill in drexel user ID here
uid = "stf45@drexel.edu"
params = urllib.parse.quote_plus(f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:netsdata.database.windows.net,1433;Database=NETS;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), echo=False)
# engine.connect()


# engine.execute("SELECT * FROM ClassifiedLongSample").fetchall()
Base = automap_base()
metadata = MetaData()
metadata.reflect(bind=engine)
# Base.prepare(engine, reflect=True)

#%% CREATE TABLE CLASSES AND IMPORT FUNCTION

class Address(Base):

    __tablename__ = "Address"


class BusinessInfo(Base):

    __tablename__ = "BusinessInfo"


class DunsMove(Base):

    __tablename__ = "DunsMove"


class Category(Base):

    __tablename__ = "Category"


class Classification(Base):

    __tablename__ = "Classification"


class BG_CC_TC_Xwalk(Base):

    __tablename__ = "BG_CC_TC_Xwalk"


class DunsLocation(Base):

    __tablename__ = "DunsLocation"


Base.prepare(autoload_with=engine)

Session = sessionmaker(bind=engine)
session = Session()

def import_table(df, table, drop=None, rename=None, **kwargs):

    if isinstance(df, str):
        data = pd.read_csv(df, sep="\t", dtype=kwargs.get("dtype", None)).replace({"": None, np.nan: None})
    else:
        data = df.copy()

    str_cols = data.select_dtypes(["object"])

    data[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())

    if drop:
        data.drop(drop, inplace=True, axis=1)
    
    if rename:
        data.rename(columns=rename, inplace=True)

    for d in data.to_dict(orient="records"):
        session.add(table(**d))
    
    session.commit()

#%%

with open(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230726.txt") as file:
    reader = csv.reader(delimiter='\t')
    for row in reader:
        yield row 







#%%
address = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230608.txt", sep="\t",
                      dtype={"GcZIP4": str, "GcZIP": str}, 
                      #chunksize=chunksize
                      # nrows=10
                      )

str_cols = address.select_dtypes(["object"])

address[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())    

address.to_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230726.txt", sep="\t", index=False)

#%%
xwalk = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BG_CC_TC_Xwalk20230613.txt", sep="\t",
                      # dtype={"GcZIP4": str, "GcZIP": str}, 
                      #chunksize=chunksize
                      # nrows=10
                      )

str_cols = xwalk.select_dtypes(["object"])

xwalk[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())    

xwalk.to_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BG_CC_TC_Xwalk20230802.txt", sep="\t", index=False)

#%%

cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:netsdata.database.windows.net,1433;Database=NETS;Initial Catalog=NETS;Persist Security Info=False;User ID=stf45@drexel.edu;MultipleActiveResultSets=False;Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryInteractive;')
cn = cnxn.cursor()

#%%
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

n=23254659
chunksize = 10000
address = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230726.txt", sep="\t",
                      dtype={"GcZIP4": str, "GcZIP": str}, 
                      chunksize=chunksize
                      # nrows=100
                      )

for c,chunk in enumerate(address):
    cn.executemany("INSERT INTO Address (AddressID,	Address, City, State, Zip, Zip4) "
               "VALUES (?,?,?,?,?,?)", chunk[['AddressID', 'GcAddress', 'GcCity', 'GcState', 'GcZip', 'GcZip4']].values.tolist())
    cnxn.commit()
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('{}: chunk {} completed in {} minutes! {} chunks to go'.format(datetime.now().strftime("%H:%M:%S"),c+1, round(t/60, 2), round(n/chunksize-(c+1),2)))    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% IMPORT ADDRESS TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 1000000
address = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230608.txt", sep="\t",
                      dtype={"GcZIP4": str, "GcZIP": str}, chunksize=chunksize)

for chunk in address:
    chunk = chunk.replace({"": None, np.nan: None}).rename(columns={"GcAddress": "Address", "GcCity": "City", "GcState": "State", "GcZIP": "Zip", "GcZIP4": "Zip4"})
    chunk["Zip"] = chunk["Zip"].str.zfill(5)
    chunk["Zip4"] = chunk["Zip4"].astype(float).fillna(-99).apply(floor).astype(str).replace("-99", None).str.zfill(4)
    import_table(chunk, Address)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  
del chunk

#%% IMPORT BUSINESS INFO TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
businessinfo = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BusinessInfoDB20230710.txt", sep="\t", chunksize=chunksize)

for chunk in businessinfo:
    import_table(chunk, BusinessInfo, rename={"YearFull": "Year"})
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% IMPORT DUNSMOVE DUNSYEAR KEY TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
dunsmove = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/DunsMove_DunsYear_Key20230711.txt", sep="\t", chunksize=chunksize)

for chunk in dunsmove:
    import_table(chunk, DunsMove)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% IMPORT CATEGORY DESCRIPTIONS TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
category = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/CategoryDescriptions20230613.txt", sep="\t", chunksize=chunksize)

for chunk in category:
    import_table(chunk, Category)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)  

#%% IMPORT CLASSIFICATION TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
classification = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/ClassifiedLong20230506.txt", sep="\t", chunksize=chunksize)

for chunk in classification:
    import_table(chunk, Classification)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% IMPORT BG CC TC XWALK TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
xwalk = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/ClassifiedLong20230506.txt", sep="\t", chunksize=chunksize)

for chunk in xwalk:
    import_table(chunk, BG_CC_TC_Xwalk)
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% IMPORT DUNSLOCATION TABLE

print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

chunksize = 10000000
dunsloc = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/DunsLocations20230608.csv", sep="\t", chunksize=chunksize)

for chunk in dunsloc:
    import_table(chunk, DunsLocation, rename={"POINT_X": "Xcoord", "POINT_Y": "Ycoord", "area10km": "TotalArea", "ALAND10km": "AreaLand"})
    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

