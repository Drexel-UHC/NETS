# %%
import sqlalchemy
import urllib
import pandas as pd

from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey, select


# %%
uid = "[insert user name]"
params = urllib.parse.quote_plus(f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:uhcdata.database.windows.net,1433;Database=NETSSample;UID={uid};MultipleActiveResultSets=False;Authentication=ActiveDirectoryInteractive;")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), echo=False)
session = Session(engine)

# %%
Base = automap_base()
metadata = MetaData(bind=engine)

# %%
class GeocodedSample(Base):

    __tablename__ = "GeocodedSample"

class DunsMoveYearSample(Base):

    __tablename__ = "DunsMoveYearSample"

class ClassifiedLongSample(Base):

    __tablename__ = "ClassifiedLongSample"
# %%

Base.prepare(autoload_with=engine)

# %%
## with session.query()
output = session.query(GeocodedSample.DunsMove, GeocodedSample.DisplayX, GeocodedSample.DisplayY, GeocodedSample.DunsNumber,
                       GeocodedSample.Status, GeocodedSample.Score, GeocodedSample.Match_addr, DunsMoveYearSample.DunsYear,
                       ClassifiedLongSample.Category)\
    .join(DunsMoveYearSample, GeocodedSample.DunsMove == DunsMoveYearSample.DunsMove, isouter=True)\
    .join(ClassifiedLongSample, DunsMoveYearSample.DunsYear == ClassifiedLongSample.DunsYear, isouter=True)

df5=pd.DataFrame(output.all())

# %%
## with session.execute(select)

output = session.execute(select(GeocodedSample.DunsMove, GeocodedSample.DisplayX, GeocodedSample.DisplayY, GeocodedSample.DunsNumber,
                                GeocodedSample.Status, GeocodedSample.Score, GeocodedSample.Match_addr, DunsMoveYearSample.DunsYear,
                                ClassifiedLongSample.Category)\
                        .join(DunsMoveYearSample, GeocodedSample.DunsMove == DunsMoveYearSample.DunsMove, isouter=True)\
                        .join(ClassifiedLongSample, DunsMoveYearSample.DunsYear == ClassifiedLongSample.DunsYear, isouter=True))
pd.DataFrame(output.all())

# %%