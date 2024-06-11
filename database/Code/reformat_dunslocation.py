# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 10:52:01 2024

@author: stf45
"""

import pandas as pd

dtype = {'DunsLocationId':int,
'Xcoord':float,
'Ycoord':float,
'DisplayX':float,
'DisplayY':float,
'GEOID10':pd.Int64Dtype(),
'TractLandArea':float,
'TractTotalArea':float,
'ct10_distance':float,
'ZCTA5CE10':pd.Int64Dtype(),
'ZCTALandArea':float,
'ZCTATotalArea':float,
'zcta10_distance':float,
'Addr_type':str,
'Status':str,
'Score':float,
'UHCMatchCodeRank':pd.Int64Dtype(),
'AddressID':str,
'GcAddress':str,
'GcCity':str,
'GcState':str,
'GcZIP':pd.Int64Dtype(),
'GcZIP4':pd.Int64Dtype()}

dl = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\DB_Optimization\MergedLocationTable.txt', dtype=dtype, sep='\t')

# check to see if columns
cols = list(dl.columns)
for col in cols:
    # get max length of value in string col
    if dl[col].dtype == "O":
        print(col, dl[col].str.len().max())
    else:
        pass

# make formatting changes
roundcols = ['TractLandArea', 'TractTotalArea', 'ZCTALandArea', 'ZCTATotalArea']
censuscols = {'GEOID10':11, 'ZCTA5CE10':5, 'GcZIP':5, 'GcZIP4':4}
for col in cols:
    print(col)
    # round some columns to 2 decimal pts
    if col in roundcols:
        dl[col] = dl[col].round(2)
    # fill in leading zeros in census id cols
    elif col in censuscols.keys():
        print(censuscols[col])
        dl[col] = (dl[col]
                   .astype(str)
                   .str
                   .zfill(censuscols[col])
                   ) 
    else:
        pass
    
dlhead = dl.head(1000)

dl.to_csv(r'D:\scratch\MergedLocationTableReformat.txt', sep='\t', index=False)

