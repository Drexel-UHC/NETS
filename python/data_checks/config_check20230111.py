# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 10:09:38 2023

@author: stf45
"""

#%%

import pandas as pd
import json
import os
os.chdir(r"C:\Users\stf45\Documents\NETS\Processing\python")
import nets_functions as nf
import math
import numpy as np
import operator as op
from datetime import datetime
import time

#%% LOAD JSON CONFIG

with open(r'C:\Users\stf45\Documents\NETS\Processing\config/nets_config_20230206.json', 'r') as f:
    config = json.load(f)
    
# with open(r'C:\Users\stf45\Documents\NETS\Processing\config/json_config_2022_04_20.json', 'r') as f:
#     config2 = json.load(f)
    
#%%

catlist='''ACM
ALM
AMP
AMU
ARC
ART
AVP
AWK
BAR
BDS
BEU
BHH
BHO
BIO
BKN
BKS
BNK
BOK
CAB
CBC
CCH
CDS
CFN
CFS
CHR
CHS
CLO
CLS
CMU
CND
CNF
CNV
COM
COS
CPC
CRD
CRP
CRV
CSD
CVP
DCR
DCS
DDP
DDS
DEM
DLR
DOC
DPT
DRG
DRW
EAO
EAP
EAR
EAT
EEP
EEU
ELW
EMT
ETC
FCS
FFS
FIR
FOR
FSG
FSH
FVM
FWK
GHT
GMM
GNH
GPA
GRY
GSS
GUR
HHG
HOB
HOS
HPC
HTL
HWS
IBC
ILC
INV
IPC
IRC
IUC
JCO
KCT
LAU
LFS
LGM
LGN
LGW
LIB
LIQ
LOT
MAG
MAS
MET
MFC
MHH
MHO
MIR
MOR
MPC
MST
MUA
NAT
NCL
NPI
NSD
NUT
OFD
OFN
OPT
PBE
PCC
PET
PHT
PHV
PIZ
PLO
POL
POS
PSC
QSV
RBS
RCC
REA
REL
RES
RFS
RLG
RSC
RSI
RTC
SCB
SCC
SCL
SCN
SER
SFM
SHP
SLC
SMK
SPA
SPS
SRB
SRC
SRG
SRO
STF
STL
TAN
TAT
TAX
TOB
TOU
TRN
TRS
TSC
TVA
UNI
URG
VID
WAT
WOO
WPS
WRS
WTL
XXX
ZOO
'''
catlist = catlist.splitlines()
catlist = set(catlist)
#%%

cats = []

for x in config.keys():
    cats.append(x)

# are there duplicates of any cats?:: no
nodups = set(cats)
print(len(nodups) == len(cats))

print(nodups.difference(catlist))
print(catlist.difference(nodups))

#%%

codes=[]
catlist=[]

for x in config.keys():
    print(x)
    if math.isnan(config[x]['conditional']) == True:
        print(f'{x} has no conditional code')
    else:
        codes.append(config[x]['conditional'])
        catlist.append(x)

#%%


#%% READ IN FILES

# FULL FILES
# n = 71498225
# chunksize = 6000000

# company = r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt'
# sic = r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt'
# emp = r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt'
# sales = r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt'

# SAMPLE FILES
n = 1000
chunksize = 100

company = r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt'
sic = r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt'
emp = r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt'
sales = r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt'


company_reader = pd.read_csv(company, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, 
                             chunksize=chunksize, 
                             usecols=['DunsNumber','Company','TradeName'])
sic_reader = pd.read_csv(sic, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                "SIC90",
                                                                                                                                                                "SIC91",
                                                                                                                                                                "SIC92",
                                                                                                                                                                "SIC93",
                                                                                                                                                                "SIC94",
                                                                                                                                                                "SIC95",
                                                                                                                                                                "SIC96",
                                                                                                                                                                "SIC97",
                                                                                                                                                                "SIC98",
                                                                                                                                                                "SIC99",
                                                                                                                                                                "SIC00",
                                                                                                                                                                "SIC01",
                                                                                                                                                                "SIC02",
                                                                                                                                                                "SIC03",
                                                                                                                                                                "SIC04",
                                                                                                                                                                "SIC05",
                                                                                                                                                                "SIC06",
                                                                                                                                                                "SIC07",
                                                                                                                                                                "SIC08",
                                                                                                                                                                "SIC09",
                                                                                                                                                                "SIC10",
                                                                                                                                                                "SIC11",
                                                                                                                                                                "SIC12",
                                                                                                                                                                "SIC13",
                                                                                                                                                                "SIC14",
                                                                                                                                                                "SIC15",
                                                                                                                                                                "SIC16",
                                                                                                                                                                "SIC17",
                                                                                                                                                                "SIC18",
                                                                                                                                                                "SIC19"])

emp_reader = pd.read_csv(emp, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                "Emp90",
                                                                                                                                                                "Emp91",
                                                                                                                                                                "Emp92",
                                                                                                                                                                "Emp93",
                                                                                                                                                                "Emp94",
                                                                                                                                                                "Emp95",
                                                                                                                                                                "Emp96",
                                                                                                                                                                "Emp97",
                                                                                                                                                                "Emp98",
                                                                                                                                                                "Emp99",
                                                                                                                                                                "Emp00",
                                                                                                                                                                "Emp01",
                                                                                                                                                                "Emp02",
                                                                                                                                                                "Emp03",
                                                                                                                                                                "Emp04",
                                                                                                                                                                "Emp05",
                                                                                                                                                                "Emp06",
                                                                                                                                                                "Emp07",
                                                                                                                                                                "Emp08",
                                                                                                                                                                "Emp09",
                                                                                                                                                                "Emp10",
                                                                                                                                                                "Emp11",
                                                                                                                                                                "Emp12",
                                                                                                                                                                "Emp13",
                                                                                                                                                                "Emp14",
                                                                                                                                                                "Emp15",
                                                                                                                                                                "Emp16",
                                                                                                                                                                "Emp17",
                                                                                                                                                                "Emp18",
                                                                                                                                                                "Emp19"])

sales_reader = pd.read_csv(sales, sep = '\t', dtype={"DunsNumber": str}, encoding_errors='replace', header=0, chunksize=chunksize, usecols=["DunsNumber",
                                                                                                                                                                    "Sales90",
                                                                                                                                                                    "Sales91",
                                                                                                                                                                    "Sales92",
                                                                                                                                                                    "Sales93",
                                                                                                                                                                    "Sales94",
                                                                                                                                                                    "Sales95",
                                                                                                                                                                    "Sales96",
                                                                                                                                                                    "Sales97",
                                                                                                                                                                    "Sales98",
                                                                                                                                                                    "Sales99",
                                                                                                                                                                    "Sales00",
                                                                                                                                                                    "Sales01",
                                                                                                                                                                    "Sales02",
                                                                                                                                                                    "Sales03",
                                                                                                                                                                    "Sales04",
                                                                                                                                                                    "Sales05",
                                                                                                                                                                    "Sales06",
                                                                                                                                                                    "Sales07",
                                                                                                                                                                    "Sales08",
                                                                                                                                                                    "Sales09",
                                                                                                                                                                    "Sales10",
                                                                                                                                                                    "Sales11",
                                                                                                                                                                    "Sales12",
                                                                                                                                                                    "Sales13",
                                                                                                                                                                    "Sales14",
                                                                                                                                                                    "Sales15",
                                                                                                                                                                    "Sales16",
                                                                                                                                                                    "Sales17",
                                                                                                                                                                    "Sales18",
                                                                                                                                                                    "Sales19"])

#%% CLASSIFY

readers = zip(sic_reader, emp_reader, sales_reader, company_reader)
print(f"Start Time: {datetime.now()}")
time_list = [0]
tic = time.perf_counter()

cats=[]
symbols = {"l": [op.lt], "le": [op.le], "g": [op.gt], "ge": [op.ge], "bt":[op.ge, op.lt]}
   
for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk) in enumerate(readers):
    header = (c==0)
    classification_wide = nf.merge_sic_emp_sales(sic_chunk, emp_chunk, sales_chunk, company_chunk)
    df = nf.normal_to_long(classification_wide, header)
    for cat in config.keys():

    # CONDIT 1: record has name match
    ## check name match in either Company or TradeName columns. NO CATEGORIES HAVE THIS CONDITION; just a test.

        if config[cat]['conditional'] == 1:
            regex = '|'.join(config[cat]['name'])
            comp_match = (df['Company'].str.contains(regex))
            trade_match = (df['TradeName'].fillna("").str.contains(regex))
            cats.append(pd.DataFrame({cat: comp_match*1 + trade_match*2 }))

    # CONDIT 2: record falls in sic_exclusive OR sic_range and name match
    ##check company and trade name? currently checking company only CHANGE TO
    #ADD TRADENAME, CHECK

        elif config[cat]['conditional'] == 2:
            sic_range = nf.make_sic_range(cat,config)
            sic_exclusive = config[cat]["sic_exclusive"]
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = compmatch_bool|tradematch_bool
            range_bool = df["SIC"].isin(sic_range)
            ex_bool = df["SIC"].isin(sic_exclusive)
            match_bool[~range_bool] = False
            final_bool = match_bool|ex_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))
        
    # CONDIT 3: record falls in sic_range        
        elif config[cat]['conditional'] == 3:
            sic_range = nf.make_sic_range(cat,config)
            range_bool = df["SIC"].isin(sic_range)
            cats.append(pd.DataFrame({cat: range_bool*1 }))

    # CONDIT 4: record falls in sic_exclusive or sic_range      
        elif config[cat]['conditional'] == 4:
            valid_sics = nf.make_sic_ex_range(cat,config)
            sic_bool = df["SIC"].isin(valid_sics)
            cats.append(pd.DataFrame({cat: sic_bool*1}))
            

    # CONDIT 5: record falls in sic_range and emp NO CATEGORIES HAVE THIS CONDITION


    # CONDIT 6: record falls in sic_range AND emp or sales (emp must be non missing) (BDS only)
        
        elif config[cat]['conditional'] == 6:
            sic_range = nf.make_sic_range(cat,config)
            range_bool = df["SIC"].isin(sic_range)
            
            # check emp conditions, record is True if conditions are met
            emp_oper_type = config[cat]['emp'][0] 
            emp_oper = symbols[emp_oper_type][0]
            if emp_oper_type == "bt":
                emp_oper2 = symbols[emp_oper_type][1]
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and emp_oper2(x, config[cat]['emp'][2]) else False)
            elif emp_oper_type in list(symbols.keys())[:4]:
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and pd.notna(x) and x!=0 else False)
            else:
                print(f"unknown condition. {cat} emp condition in config: {config[cat]['emp']}")
            
            # check sales conditions, record is True if conditions are met
            sales_oper_type = config[cat]['sales'][0] 
            sales_oper = symbols[sales_oper_type][0]
            if sales_oper_type == "bt":
                sales_oper2 = symbols[sales_oper_type][1]
                sales_bool = df['Sales'].apply(lambda x: True if (sales_oper(x, config[cat]['sales'][1]) and sales_oper2(x, config[cat]['sales'][2])) else False)
            elif sales_oper_type in list(symbols.keys())[:4]:
                sales_bool = df['Sales'].apply(lambda x: True if sales_oper(x, config[cat]['sales'][1]) or (pd.isna(x) or x==0) else False)
            else:
                print(f"unknown condition. {cat} sales condition in config: {config[cat]['sales']}")

            # check all bools against each other
            emp_bool[~sales_bool] = False
            range_bool[~emp_bool] = False
            cats.append(pd.DataFrame({cat: range_bool*1}))

            
    # CONDIT 7: record falls in sic range OR sic_range_2 and name match
    # (LIQ only)
    # this currently takes the longest

        elif config[cat]['conditional'] == 7:
            sic_range = nf.make_sic_range(cat,config)
            sic_range2 = nf.make_sic_range2(cat,config)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = compmatch_bool|tradematch_bool
            range_bool = df["SIC"].isin(sic_range)
            range2_bool = df["SIC"].isin(sic_range2)
            match_bool[~range2_bool] = False
            final_bool = match_bool|range_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))


    # CONDIT 8: record falls in sic_exclusive list
        
        elif config[cat]['conditional'] == 8:
            valid_sics = config[cat]['sic_exclusive']
            sic_bool = df["SIC"].isin(valid_sics)
            cats.append(pd.DataFrame({cat: sic_bool*1}))
            
            
    # CONDIT 9: record falls in sic_exclusive OR name match

        elif config[cat]['conditional'] == 9:
            valid_sics = config[cat]['sic_exclusive']
            sic_bool = df["SIC"].isin(valid_sics)
            regex = '|'.join(config[cat]['name'])
            comp_match = (df['Company'].str.contains(regex))
            trade_match = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = comp_match|trade_match
            final_bool = match_bool|sic_bool
            cats.append(pd.DataFrame({cat: final_bool*1 }))


    # CONDIT 10: sic range AND emp AND sales, either emp or sales can be missing, but not both (GRY only)
        
        elif config[cat]['conditional'] == 10:
            sic_range = nf.make_sic_range(cat,config)
            range_bool = df["SIC"].isin(sic_range)
            
            # check emp conditions, record is True if conditions are met
            emp_oper_type = config[cat]['emp'][0] 
            emp_oper = symbols[emp_oper_type][0]
            if emp_oper_type == "bt":
                emp_oper2 = symbols[emp_oper_type][1]
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and emp_oper2(x, config[cat]['emp'][2]) else False)
            elif emp_oper_type in list(symbols.keys())[:4]:
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and pd.notna(x) and x!=0 else False)
            else:
                print(f"unknown condition. {cat} emp condition in config: {config[cat]['emp']}")
            
            # check sales conditions, record is True if conditions are met
            sales_oper_type = config[cat]['sales'][0] 
            sales_oper = symbols[sales_oper_type][0]
            if sales_oper_type == "bt":
                sales_oper2 = symbols[sales_oper_type][1]
                sales_bool = df['Sales'].apply(lambda x: True if sales_oper(x, config[cat]['sales'][1]) and sales_oper2(x, config[cat]['sales'][2]) else False)
            elif sales_oper_type in list(symbols.keys())[:4]:
                sales_bool = df['Sales'].apply(lambda x: True if sales_oper(x, config[cat]['sales'][1]) and pd.notna(x) and x!=0 else False)
            else:
                print(f"unknown condition. {cat} sales condition in config: {config[cat]['sales']}")
            
            # create two boolean series' where missing values or 0 are True
            emp_na_bool = df['Emp'].apply(lambda x: True if pd.isna(x) or x==0 else False)   
            sales_na_bool = df['Sales'].apply(lambda x: True if pd.isna(x) or x==0 else False)
            
            
            # if emp and sales are true, emp_sales is true
            # if emp and sales_na are true, emp_salesna is true
            # if sales and emp_na are true, sales_empna is true
            # if emp_sales, emp_salesna, or sales_empna are true, final bool is true
            # if range_bool and final_bool are both true, final_bool is true
            emp_sales = emp_bool & sales_bool
            emp_salesna = emp_bool & sales_na_bool
            sales_empna = sales_bool & emp_na_bool
            final_bool = emp_sales|emp_salesna|sales_empna
            final_bool[~range_bool] = False
            cats.append(pd.DataFrame({cat: final_bool*1}))
            

    # CONDIT 11: record falls in sic_exclusive OR sic_range_2 and sic_exclusive_2 and name search

        elif config[cat]['conditional'] == 11:
            sic_ex = config[cat]['sic_exclusive']
            sic_range2 = nf.make_sic_ex_range2(cat,config)
            sic_bool = df["SIC"].isin(sic_ex)
            sic_bool2 = df["SIC"].isin(sic_range2)
            regex = '|'.join(config[cat]['name'])
            
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))        
            name_bool = compmatch_bool|tradematch_bool
            sic_bool2[~name_bool] = False
            final_bool = sic_bool|sic_bool2
            cats.append(pd.DataFrame({cat: final_bool*1}))

    # CONDIT 12: record falls in sic_range and Company name OR sic_range_2 and TradeName

        elif config[cat]['conditional'] == 12:
            sic_range = nf.make_sic_range(cat,config)
            sic_range2 = nf.make_sic_range2(cat,config)
            sic_bool = df["SIC"].isin(sic_range)
            sic_bool2 = df["SIC"].isin(sic_range2)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))        
            compmatch_bool[~sic_bool] = False
            tradematch_bool[~sic_bool2] = False
            final_bool = compmatch_bool|tradematch_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))
            
    # CONDIT 13: sic range AND emp or sales, either emp or sales can be missing, but not both
        
        elif config[cat]['conditional'] == 13:
            sic_range = nf.make_sic_range(cat,config)
            range_bool = df["SIC"].isin(sic_range)
            
            # check emp conditions, record is True if conditions are met
            emp_oper_type = config[cat]['emp'][0] 
            emp_oper = symbols[emp_oper_type][0]
            if emp_oper_type == "bt":
                emp_oper2 = symbols[emp_oper_type][1]
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and emp_oper2(x, config[cat]['emp'][2]) else False)
            elif emp_oper_type in list(symbols.keys())[:4]:
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) else False)
            else:
                print(f"unknown condition. {cat} emp condition in config: {config[cat]['emp']}")
            
            # check sales conditions, record is True if conditions are met
            sales_oper_type = config[cat]['sales'][0] 
            sales_oper = symbols[sales_oper_type][0]
            if sales_oper_type == "bt":
                sales_oper2 = symbols[sales_oper_type][1]
                sales_bool = df['Sales'].apply(lambda x: True if sales_oper(x, config[cat]['sales'][1]) and sales_oper2(x, config[cat]['sales'][2]) else False)
            elif sales_oper_type in list(symbols.keys())[:4]:
                sales_bool = df['Sales'].apply(lambda x: True if sales_oper(x, config[cat]['sales'][1]) else False)
            else:
                print(f"unknown condition. {cat} sales condition in config: {config[cat]['sales']}")    
            
            # if emp or sales are true, emp_sales is true
            # if range_bool and emp_sales are both true, final_bool is true
            emp_sales = emp_bool|sales_bool
            final_bool = emp_sales & range_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))
            
    # if no conditional code found in json
        else:
            print(f"unknown condition for {cat}")
    break

#%%
    df = df.astype({'SIC':int, 'Emp':int, 'Sales':int})
    df['SIC'] = df['SIC'].astype(str).str.zfill(8)
    out_df = pd.concat(cats,axis=1)
    final_df = pd.concat([df, out_df],axis=1)
    final_df.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/classifiedtest.txt", sep="\t", header=header, mode='a', index=False)


    toc = time.perf_counter()
    t = toc - (sum(time_list) + tic)
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))
    

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

classification = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classification.txt', sep='\t', dtype = object, nrows=30000)
classified = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\classifiedtest.txt', sep='\t', dtype = object, nrows=30000)

