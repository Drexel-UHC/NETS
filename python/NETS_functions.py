# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 13:42:03 2022

@author: stf45


This file contains the functions used to wrangle and process the NETS 2019 dataset
for the Drexel MESA Neighborhoods Aging study. Functions are further documented
individually throughout this file.
"""

#%%
import pandas as pd
import numpy as np
import operator as op
from datetime import datetime

#%%
'''
This function is used in create_classification_input.py. Merge the sic, emp, sales, and company datasets. 
'''

def merge_sic_emp_sales(sic_chunk, emp_chunk, sales_chunk, company_chunk):  
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    classification_wide = pd.merge(emp_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide
    
#%%
'''
This function is used in create_classification_input.py. Swings wide dataset 
(such as that generated from merge_sic_emp_sales) into long
format, so that each record has a unique DunsNumber/Year aka "DunsYear". Output columns include:
['DunsNumber','DunsYear','Year','Company','TradeName','SIC', 'Emp','Sales']
DunsYear's with null SIC values are dropped.
'''

def normal_to_long(chunk, header):
    
    # swing SalesYY, EmpYY, SICYY columns to long, add key column "Year", value columns "Sales", "Emp", "SIC"
    long_chunk = pd.wide_to_long(chunk, stubnames=["Sales", "Emp", "SIC"], i="DunsNumber", j= "Year", suffix='\d+')
    long_chunk['Sales'] = long_chunk['Sales'].fillna(0)  
    long_chunk = long_chunk.reset_index()
    long_chunk = long_chunk.dropna(subset=['SIC'])

    
    # create list with four digit years 
    YearFull = []
    for num in long_chunk["Year"]:
        if len(str(num)) == 1:
            YearFull.append("200" + str(num))
        elif num < 90: 
            YearFull.append("20" + str(num))        
        else:
            YearFull.append("19" + str(num))
        
    
    # for each dataframe, add four digit years to dataframe, add new column with dunsnumber + year
    long_chunk["YearFull"] = YearFull
    long_chunk["DunsYear"] = long_chunk["DunsNumber"] + "_" + long_chunk["YearFull"]
    
    # merge dataframes keeping only necessary columns and removing duplicates; removing rows with null values in "Sales" column
    long_chunk = long_chunk.drop(columns=['Year'])
    long_chunk = long_chunk.rename(columns={'YearFull':'Year'})
    long_chunk = long_chunk.astype({'Sales': int, 'Emp': int, 'SIC': int})
    long_chunk = long_chunk[['DunsYear','DunsNumber','Year','Company','TradeName','SIC', 'Emp','Sales']]
    # output to csv
    long_chunk.to_csv(r"D:\scratch/classification_inputYYYYMMDD.txt", sep="\t", header=header, mode='a', index=False)
    return long_chunk


#%% GEOCODING WRANGLE 

'''
This function is used in create_geocoding.py

-takes one of the geocoding dataset halves (queried in the next cell as either 
"DunsNumber <= 100000000" or "DunsNumber > 100000000"), merges it with the 
first_last dataset so each dunsnumber/address gets a first year ('FirstYear') 
and last year ('LastYear') that the dunsnumber was in business
-applies the LastYear to the LastYear column for all of the most recent 
dunsnumber/addresses
-creates a FirstYear for all dunsnumber/addresses using either FirstYear
(if it is the first address) or the previous address' LastYear + 1
-drop unnecessary columns and reorder
-write out to csv "geocoding_2"
'''

def geocoding_wrangle(geocoding_1_half, first_last, header):
    geocoding_2 = geocoding_1_half.merge(first_last, on='DunsNumber')
    
    # use zfill to replace leading zeros in dunsnumbers
    geocoding_2['DunsNumber'] = geocoding_2['DunsNumber'].astype(str).str.zfill(9)
    
    # create column geocoding_2["MoveNum"] that indicates 0 for most recent address, 
    #1 for second to last address, 2 for third to last address, etc
    # create column geocoding_2['DunsMove'] that concatenates 1 + movenum + dunsnumber
    movenum = []
    
    geocoding_2 = geocoding_2.sort_values(by=['DunsNumber', 'GcLastYear'], ascending=[True, False])
    geocoding_2['dunslag'] = geocoding_2['DunsNumber'].shift(1)
    for i, row in geocoding_2.iterrows():
        if row['DunsNumber'] != row['dunslag']:
            count=0
            movenum.append(count)
        else:
            count += 1
            movenum.append(count)
    
    geocoding_2['MoveNum'] = movenum
    geocoding_2['DunsMove'] = (10 + geocoding_2['MoveNum']).astype(str)
    geocoding_2['DunsMove'] = geocoding_2['DunsMove'] + geocoding_2['DunsNumber']
    
    # set values in 'GcLastYear' == 3000 to value in 'LastYear', all other values equal
    # This applies the last year of a dunsnumber's existence (last year that data
    #is given) to the 'GcLastYear' for all addresses from the company dataset,
    #which represent only the most recent addresses. The GcLastYear's for the 
    #other (move) addresses are derived from move['MoveYear']
    geocoding_2['GcLastYear'] = np.where(geocoding_2['GcLastYear'] == 3000, geocoding_2['LastYear'], geocoding_2['GcLastYear']).astype(int)

    
    # group by dunsnumber to find lowest GcLastYear, "min". create a lag of GcLastYear
    geocoding_2['min'] = geocoding_2['GcLastYear'].groupby(geocoding_2['DunsNumber']).transform('min')
    geocoding_2['GcLastLag'] = geocoding_2['GcLastYear'].shift(-1)
    
    # create GcFirstYear. where a record contains the lowest GcLastYear for that
    #DunsNumber,  
    geocoding_2['GcFirstYear'] = np.where(geocoding_2['GcLastYear'] == geocoding_2['min'], geocoding_2['FirstYear'], geocoding_2['GcLastLag'] + 1).astype(int)
    
    geocoding_2 = geocoding_2.drop(columns=['dunslag', 'min', 'GcLastLag', 'FirstYear', 'LastYear'])
    

    geocoding_2 = geocoding_2[['DunsNumber',
                                    'DunsMove',
                                    'MoveNum',
                                    'GcFirstYear',
                                    'GcLastYear',
                                    'GcAddress',
                                    'GcCity',
                                    'GcState',
                                    'GcZIP',
                                    'GcZIP4'
                                    ]]
    
    
    geocoding_2.to_csv(r"D:\scratch/geocoding_2_YYYYMMDD.txt", sep="\t", mode='a', header=header, index=False)
 

#%%  SIC RANGE FUNCTIONS 

'''
These functions are used in classify.py.

these functions create lists of every possible value in SIC ranges provided in
the configuration file. SIC exclusives are included when provided.
'''

def make_sic_range(cat,config):
    sic_range = []
    for i in zip(config[cat]["sic_range"][0::2], config[cat]["sic_range"][1::2]):
        sic_range.extend(list(range(i[0], i[1]+1)))
    return sic_range


def make_sic_range2(cat,config):
    sic_range_2 = []
    for i in zip(config[cat]["sic_range_2"][0::2], config[cat]["sic_range_2"][1::2]):
        sic_range_2.extend(list(range(i[0], i[1]+1)))
    return sic_range_2

def make_sic_ex_range(cat,config):
    valid_sics = []
    for i in zip(config[cat]["sic_range"][0::2], config[cat]["sic_range"][1::2]):
        valid_sics.extend(list(range(i[0], i[1]+1)))
    valid_sics.extend(config[cat]["sic_exclusive"])
    return valid_sics

def make_sic_ex_range2(cat,config):
    valid_sics = []
    for i in zip(config[cat]["sic_range_2"][0::2], config[cat]["sic_range_2"][1::2]):
        valid_sics.extend(list(range(i[0], i[1]+1)))
    valid_sics.extend(config[cat]["sic_exclusive_2"])
    return valid_sics


#%% CLASSIFY FUNCTION

'''
This function is used in classify.py.

This function takes in the long version of the NETS 2019 dataset and classifies
each DunsYear into a 3 letter (3LTR) category. Each category has different
conditions, utilizing various combinations of the SIC, Emp, Sales, Company, 
and TradeName Variables. 

The output is eight files with two columns each, one as DunsYear, and the other 
as the category name, with a value of 1 indicating category status. DunsYears 
not flagged in category are absent from the output. Most, but not all categories are mutually exclusive. 
This function (and classify.py) ONLY CATEGORIZES BASE GROUPS. Thematic Constructs 
and Combination Categories will be classified based on Base Group status at a 
later step.

"symbols" variable is used to change out conditions marked in the config file (less than, 
greater than or equal to, etc). Code using symbols may need to be reworked if 
conditions are changed in the future. 
'''


symbols = {"l": [op.lt], "le": [op.le], "g": [op.gt], "ge": [op.ge], "bt":[op.ge, op.lt]}
    
def classify(df, config, header):
    cats = []
    for cat in config.keys():
    
    # CONDIT 1: record has name match
    ## check name match in either Company or TradeName columns. NO CATEGORIES HAVE THIS CONDITION; just a test.
    
        if config[cat]['conditional'] == 1:
            regex = '|'.join(config[cat]['name'])
            comp_match = (df['Company'].str.contains(regex))
            trade_match = (df['TradeName'].fillna("").str.contains(regex))
            cats.append(pd.DataFrame({cat: comp_match*1 + trade_match*2 }))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 2: record falls in sic_exclusive OR sic_range and name match
    ##check company and trade name? currently checking company only CHANGE TO
    #ADD TRADENAME, CHECK
    
        elif config[cat]['conditional'] == 2:
            sic_range = make_sic_range(cat,config)
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
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))

    # CONDIT 3: record falls in sic_range        
        elif config[cat]['conditional'] == 3:
            sic_range = make_sic_range(cat,config)
            range_bool = df["SIC"].isin(sic_range)
            cats.append(pd.DataFrame({cat: range_bool*1 }))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 4: record falls in sic_exclusive or sic_range      
        elif config[cat]['conditional'] == 4:
            valid_sics = make_sic_ex_range(cat,config)
            sic_bool = df["SIC"].isin(valid_sics)
            cats.append(pd.DataFrame({cat: sic_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 5: record falls in sic_range and emp NO CATEGORIES HAVE THIS CONDITION
    
    
    # CONDIT 6: record falls in sic_range AND emp or sales (emp must be non missing) (BDS only)
        
        elif config[cat]['conditional'] == 6:
            sic_range = make_sic_range(cat,config)
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
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 7: record falls in sic range/exclusive and name match
    # (SPN only)
    # this currently takes the longest
    
        elif config[cat]['conditional'] == 7:
            valid_sics = make_sic_ex_range(cat,config)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = compmatch_bool|tradematch_bool
            range_bool = df["SIC"].isin(valid_sics)
            match_bool[~range_bool] = False
            cats.append(pd.DataFrame({cat: match_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 8: record falls in sic_exclusive list
        
        elif config[cat]['conditional'] == 8:
            valid_sics = config[cat]['sic_exclusive']
            sic_bool = df["SIC"].isin(valid_sics)
            cats.append(pd.DataFrame({cat: sic_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
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
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 10: sic range AND emp AND sales, either emp or sales can be missing, but not both (GRY only)
        
        elif config[cat]['conditional'] == 10:
            sic_range = make_sic_range(cat,config)
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
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 11: record falls in sic_exclusive OR sic_range_2 or sic_exclusive_2 and name search
    
        elif config[cat]['conditional'] == 11:
            sic_ex = config[cat]['sic_exclusive']
            sic_range2 = make_sic_ex_range2(cat,config)
            sic_bool = df["SIC"].isin(sic_ex)
            sic_bool2 = df["SIC"].isin(sic_range2)
            regex = '|'.join(config[cat]['name'])
            
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))        
            name_bool = compmatch_bool|tradematch_bool
            sic_bool2[~name_bool] = False
            final_bool = sic_bool|sic_bool2
            cats.append(pd.DataFrame({cat: final_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 12: record falls in sic_range and Company name OR sic_range_2 and TradeName
    
        elif config[cat]['conditional'] == 12:
            sic_range = make_sic_range(cat,config)
            sic_range2 = make_sic_range2(cat,config)
            sic_bool = df["SIC"].isin(sic_range)
            sic_bool2 = df["SIC"].isin(sic_range2)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))        
            compmatch_bool[~sic_bool] = False
            tradematch_bool[~sic_bool2] = False
            final_bool = compmatch_bool|tradematch_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # CONDIT 13: sic range AND emp or sales, either emp or sales can be missing, but not both
        
        elif config[cat]['conditional'] == 13:
            sic_range = make_sic_range(cat,config)
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
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
        
    # CONDIT 14: SAS coded cats. will be passed.
        elif config[cat]['conditional'] == 14:
            pass
        
    # CONDIT 15: sic range and name (DLR, PIN, others)
    
        elif config[cat]['conditional'] == 15:
            sic_range = make_sic_range(cat,config)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = compmatch_bool|tradematch_bool
            range_bool = df["SIC"].isin(sic_range)
            match_bool[~range_bool] = False
            cats.append(pd.DataFrame({cat: match_bool*1}))
            # print(cat, config[cat]['conditional'], datetime.now().strftime("%H:%M:%S"))
            
    # UNKNOWN CODE: if unknown/empty conditional code found in json
        else:
            print(f"unknown condition for {cat}")
            break
        
    df = df.astype({'SIC':int, 'Emp':int, 'Sales':int})
    df['SIC'] = df['SIC'].astype(str).str.zfill(8)
    out_df = pd.concat(cats,axis=1)
    final_df = pd.concat([df['DunsYear'], out_df],axis=1)
    # get rid of any unclassified rows (has all zeros)
    final_df = final_df.loc[~(final_df.iloc[:, 1:]==0).all(axis=1)]
    final_df.to_csv(r"D:\scratch/classified_pythonYYYYMMDD.txt", sep="\t", header=header, mode='a', index=False)

