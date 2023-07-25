import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey
import urllib
import numpy as np
from math import floor


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


# change out filepaths*****************
address = pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/Addresses20230608.txt", sep="\t",
                      dtype={"GcZIP4": str, "GcZIP": str}).replace({"": None, np.nan: None}).rename(columns={"GcAddress": "Address", "GcCity": "City", "GcState": "State", "GcZIP": "Zip", "GcZIP4": "Zip4"})
address["Zip"] = address["Zip"].str.zfill(5)
address["Zip4"] = address["Zip4"].astype(float).fillna(-99).apply(floor).astype(str).replace("-99", None).str.zfill(4)
import_table(address, Address) #started at 4:53
del address
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BusinessInfoDBsample.txt", BusinessInfo, rename={"YearFull": "Year"})
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/DunsMove_DunsYear_Key20230711.txt", DunsMove)
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/CategoryDescriptions20230613.txt", Category)
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/ClassifiedLong20230506.txt", Classification)
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/BG_CC_TC_Xwalk20230613.txt", BG_CC_TC_Xwalk)
import_table(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python/DunsLocations20230608.csv", DunsLocation, drop=["ct10_distance",],
             rename={"POINT_X": "Xcoord", "POINT_Y": "Ycoord", "area10km": "TotalArea", "ALAND10km": "AreaLand"})