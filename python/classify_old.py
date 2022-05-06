
import pandas as pd
import numpy as np
import json 
import time
import operator as op
import warnings

# filter warnings from regex search
warnings.filterwarnings("ignore", category=UserWarning)

'''
This script takes in the classification.txt file and classifies each record by 
a 3 letter (MESA neighborhoods? BEH?) category. The result is a table including 
boolean columns for each 3 letter category that is appended onto the original 
input file

Input:
    classification.txt
Output:
    NETS_coded.txt

'''
#%% LOAD JSON AND NETS DATA

with open('config/json_config_2022_04_20_MAR.json', 'r') as f:
    config = json.load(f)

df = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt", sep='\t', dtype={'DunsNumber': str})

#%%  SIC RANGE FUNCTIONS 

def make_sic_range(cat):
    sic_range = []
    for i in zip(config[cat]["sic_range"][0::2], config[cat]["sic_range"][1::2]):
        sic_range.extend(list(range(i[0], i[1]+1)))
    return sic_range


def make_sic_range2(cat):
    sic_range_2 = []
    for i in zip(config[cat]["sic_range_2"][0::2], config[cat]["sic_range_2"][1::2]):
        sic_range_2.extend(list(range(i[0], i[1]+1)))
    return sic_range_2

def make_sic_ex_range(cat):
    valid_sics = []
    for i in zip(config[cat]["sic_range"][0::2], config[cat]["sic_range"][1::2]):
        valid_sics.extend(list(range(i[0], i[1]+1)))
    valid_sics.extend(config[cat]["sic_exclusive"])
    return valid_sics

def make_sic_ex_range2(cat):
    valid_sics = []
    for i in zip(config[cat]["sic_range_2"][0::2], config[cat]["sic_range_2"][1::2]):
        valid_sics.extend(list(range(i[0], i[1]+1)))
    valid_sics.extend(config[cat]["sic_exclusive_2"])
    return valid_sics

#%% CLASSIFY FUNCTION

# THOUGHTS: 
    # would turning sic_range, sic_exclusive into numpy arrays improve performance?
    ## not sure of the proper syntax to achieve the same effect
    # how is this thing gonna work when I send chunks through it?


symbols = {"l": [op.lt], "le": [op.le], "g": [op.gt], "ge": [op.ge], "bt":[op.ge, op.lt]}
    
def classify(df, header):
    
    cats = []
    for cat in config.keys():
    
    # CONDIT 1: record has name match
    ## check name match in either Company or TradeName columns? NO CATEGORIES HAVE THIS CONDITION; just a test.
    
        if config[cat]['conditional'] == 1:
            regex = '|'.join(config[cat]['name'])
            comp_match = (df['Company'].str.contains(regex))
            trade_match = (df['TradeName'].fillna("").str.contains(regex))
            cats.append(pd.DataFrame({cat: comp_match*1 + trade_match*2 }))
    
    # CONDIT 2: record falls in sic_exclusive OR sic_range and name match
    ## check company and trade name? currently checking company only
    
        elif config[cat]['conditional'] == 2:
            sic_range = make_sic_range(cat)
            sic_exclusive = config[cat]["sic_exclusive"]
            regex = '|'.join(config[cat]['name'])
            match_bool = (df['Company'].str.contains(regex))
            range_bool = df["SIC"].isin(sic_range)
            ex_bool = df["SIC"].isin(sic_exclusive)
            match_bool[~range_bool] = False
            final_bool = match_bool|ex_bool
            cats.append(pd.DataFrame({cat: final_bool*1}))
                
    # CONDIT 3: record falls in sic_range        
        elif config[cat]['conditional'] == 3:
            sic_range = make_sic_range(cat)
            range_bool = df["SIC"].isin(sic_range)
            cats.append(pd.DataFrame({cat: range_bool*1 }))
    
    # CONDIT 4: record falls in sic_exclusive or sic_range      
        elif config[cat]['conditional'] == 4:
            valid_sics = make_sic_ex_range(cat)
            sic_bool = df["SIC"].isin(valid_sics)
            cats.append(pd.DataFrame({cat: sic_bool*1}))
            
    
    # CONDIT 5: record falls in sic_range and emp NO CATEGORIES HAVE THIS CONDITION
    
    
    # CONDIT 6: record falls in sic_range AND emp or sales (sales and emp must be non missing)
        
        elif config[cat]['conditional'] == 6:
            sic_range = make_sic_range(cat)
            range_bool = df["SIC"].isin(sic_range)
            
            # check emp conditions, record is True if conditions are met
            emp_oper_type = config[cat]['emp'][0] 
            emp_oper = symbols[emp_oper_type][0]
            if emp_oper_type == "bt":
                emp_oper2 = symbols[emp_oper_type][1]
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and emp_oper2(x, config[cat]['emp'][2]) else False)
            elif emp_oper_type in list(symbols.keys())[:4]:
                emp_bool = df['Emp'].apply(lambda x: True if emp_oper(x, config[cat]['emp'][1]) and (pd.notna(x) or x!=0) else False)
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
            sic_bool[~emp_bool] = False
            cats.append(pd.DataFrame({cat: sic_bool*1}))
    
            
    # CONDIT 7: record falls in sic range OR sic_range_2 and name match
    ## use Company, Tradename, both for this condit? (LIQ only)
    # this currently takes the longest
    
        elif config[cat]['conditional'] == 7:
            sic_range = make_sic_range(cat)
            sic_range2 = make_sic_range2(cat)
            regex = '|'.join(config[cat]['name'])
            compmatch_bool = (df['Company'].str.contains(regex))
            tradematch_bool = (df['TradeName'].fillna("").str.contains(regex))
            match_bool = compmatch_bool|tradematch_bool
            # make sic_range2 numpy array to improve performance?
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
    
    
    # CONDIT 10: sic range AND emp AND sales, either emp or sales can be missing, but not both
        
        elif config[cat]['conditional'] == 10:
            sic_range = make_sic_range(cat)
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
            sic_range2 = make_sic_ex_range2(cat)
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
            sic_range = make_sic_range(cat)
            sic_range2 = make_sic_range2(cat)
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
            sic_range = make_sic_range(cat)
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
            cats.append(pd.DataFrame({cat: np.zeros(df.shape[0])}))
    
    df['SIC'] = df['SIC'].astype(str).str.zfill(8)
    out_df = pd.concat(cats,axis=1)
    final_df = pd.concat([df, out_df],axis=1)
    final_df.to_csv("scratch/NETS_coded_sample.txt", sep="\t", header=header, mode='a', index=False)


#%% RUN CHUNKS THROUGH CLASSIFY FUNCTION

csv = r'C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt'
chunksize=4000
n=7681
classification_reader = pd.read_csv(csv, sep = '\t', dtype={"DunsNumber": str, 'SIC': int, 'Emp': int}, chunksize=chunksize, header=0)


for i,chunk in enumerate(classification_reader):
    tic = time.perf_counter()
    header = (i==0)
    classify(chunk,header)
    toc = time.perf_counter()
    t = toc - tic
    print('\nchunk {} completed in {} minutes. \n{} chunks to go \napprox {} minutes left'.format(i+1, round(t/60,3), round(n/chunksize-(i+1),2), round((n/chunksize-(i+1))*t/60, 2)))

#%% MAKE TEST DF


# data = {'DunsNumber': ['1','2','3','4'], 'SIC': [64110001,54110001,54110001,54110001], 'Company':["d","f","g","h"], 'TradeName':["","","",""], 'Emp': [25,26,0,np.nan], 'Sales': [21000000, 30000, 3300000, 9000] }
# df = pd.DataFrame(data)

# classify(df, 1)

coded = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_coded.txt', sep='\t', header = 0)

codedMAR = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\NETS_coded_sample.txt', sep='\t', header = 0)

codedMAR.drop(columns=['MAR'], inplace=True)

