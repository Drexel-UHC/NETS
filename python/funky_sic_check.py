# -*- coding: utf-8 -*-
"""
Created on Thu May  5 14:23:14 2022

@author: stf45
"""

import pandas as pd
import time

#%% CREATE LIST OF SICS AS FLOATS (to match sics read in file)

nums = '''01399906
01619904
07520203
07520204
07520300
07520301
07529901
15210000
15210100
15210101
15210102
15210103
15210104
15219900
15219901
15219902
15219903
15220000
15220100
15220101
15220102
15220103
15220104
15220105
15220106
15220107
15220200
15220201
15220202
17210100
17210101
17210102
17210304
17210402
17310000
17319903
17319904
17410000
17410100
17410101
17410102
17419900
17419901
17419902
17419903
17419904
17419905
17419906
17419907
17419908
17419909
17420000
17420100
17420101
17420102
17420103
17420104
17420105
17420200
17420201
17420202
17420203
17420204
17430000
17439900
17439901
17439902
17439903
17439904
17510000
17510100
17510101
17510102
17510200
17510201
17510202
17519900
17519901
17520000
17529900
17529901
17529902
17529903
17529904
17529905
17529906
17529907
17529908
17610000
17610100
17610101
17610102
17610103
17610104
17619900
17619901
17619902
17619903
17619904
17619905
17710000
17710100
17710101
17710102
17710103
17719903
17719904
17719905
17949900
17949901
17950000
17959900
17990200
17990201
17990202
17990204
17990205
17990206
17990208
17990209
17990210
17990600
17990601
17990602
17990603
17990604
17990605
17990606
17990608
17990609
17990610
17990611
17990612
17990613
17999926
39440300
39440301
39440302
39440303
39440304
39440305
39440306
39440307
39440308
39440309
39440310
39440311
39440312
39440313
39440314
39440315
41110000
41110100
41110101
41110102
41110200
41110201
41110202
41110300
41110301
41110302
41110303
41110400
41110401
41110402
41110403
41119900
41119901
41119902
41119903
41190000
41190100
41190101
41190102
41190103
41199900
41199901
41199902
41199903
41199904
41199905
41199906
41210000
41310000
41319900
41319901
41319902
41319903
47290000
47290100
47290101
47290102
47290103
47290104
47290105
47299900
47299901
50450000
50450100
50450101
50450102
50450103
50450104
50459900
50459901
50459902
50459903
50459904
50459905
50459906
50459907
50470306
50470307
50470308
50470309
50470310
62820000
62829900
62829901
62829902
62829903
62829904
62829905
65310100
65310101
65310102
65310103
65310104
65310105
65310106
65310200
70320100
70320101
70320102
70320103
70320300
70330000
70339900
70339901
70339902
70339903
70339904
75210000
75210100
75210101
75210102
75210200
75210201
75210202
75210203
79979900
80219900
80499901
80499903
80499912
80599902
80820000
80829900
80829901
80829902
80939901
80990202
80990203
82110301
83610300
83610301
83610302
83610303
86410000
86410400
86410402
86410403
86999903
86999907
89990100
89990101
89990102
89990103
89990104
89990200
89990201
89990202
89990203
'''

numslist = nums.splitlines()
numslist = [*map(float, numslist)]

#%%

def merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk):
    sic_merge = sic_chunk.merge(company_chunk, on='DunsNumber')
    emp_merge = sic_merge.merge(emp_chunk, on='DunsNumber')
    misc_merge = emp_merge.merge(misc_chunk, on='DunsNumber')
    classification_wide = pd.merge(misc_merge, sales_chunk, on='DunsNumber', how='left')
    return classification_wide

#%%

# SAMPLE FILES
# n = 1000
# chunksize = 100
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, 
#                                                                                                                                                   chunksize=chunksize, 
#                                                                                                                                                   usecols=['DunsNumber','Company','TradeName'])

# sic_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype={"DunsNumber": str, 'SIC19': float},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                               "SIC19"])

# emp_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                               "Emp19"])

# sales_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",       
#                                                                                                                                                                                   "Sales19"])
# company_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, usecols=["DunsNumber",
#                                                                                                                                                                                     "Company",
#                                                                                                                                                                                     "TradeName",
#                                                                                                                                                                                     "Address",
#                                                                                                                                                                                     "City",
#                                                                                                                                                                                     "State",
#                                                                                                                                                                                     "ZipCode"])
# misc_reader = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt', sep = '\t', dtype={"DunsNumber":str},  header=0, chunksize=chunksize, usecols=["DunsNumber", "Latitude", "Longitude"])



# FULL FILES:           
n = 71498225
chunksize = 10000000

sic_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                              "SIC19"])

                                                                                                                                                          
company_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber",
                                                                                                                                                                    "Company",
                                                                                                                                                                    "TradeName",
                                                                                                                                                                    "Address",
                                                                                                                                                                    "City",
                                                                                                                                                                    "State",
                                                                                                                                                                    "ZipCode"])
emp_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Emp19"])

                                                                                                                                                                                  

sales_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Sales19"])
                      
misc_reader = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Misc.txt', sep = '\t', dtype={"DunsNumber": str},  header=0, chunksize=chunksize, encoding_errors='replace', usecols=["DunsNumber", "Latitude", "Longitude"])
                                                                                                                                                          
                                                                                                                                        
#%% FILTER SICS, MERGE ALL FILES, APPEND TO CSV IN CHUNKS

readers = zip(sic_reader, emp_reader, sales_reader, company_reader, misc_reader)
time_list = []

for c, (sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk) in enumerate(readers):
    tic = time.perf_counter()
    header = (c==0)
    sic_chunk = sic_chunk[sic_chunk['SIC19'].isin(numslist)]
    sic_chunk= sic_chunk.astype({'SIC19':int})
    funky_sic_check_wide = merge_sic_emp_sales_misc(sic_chunk, emp_chunk, sales_chunk, company_chunk, misc_chunk)
    funky_sic_check_wide.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/scratch/funky_sic_check.txt", sep="\t", header=header, mode='a', index=False)
    toc = time.perf_counter()
    t = toc - tic
    time_list.append(t)
    print('chunk {} completed in {} minutes! {} chunks to go'.format(c+1, round(t/60, 2), n/chunksize-(c+1)))

runtime = 'total time: {} minutes'.format(round(sum(time_list)/60,2))
print(runtime)

#%% READ IN CSV, ADD BACK LEADING ZEROS TO SICS, FILTER FAMILIAR CITIES

funky_sics = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\scratch\funky_sic_check.txt', sep = '\t', dtype={"DunsNumber": str})

funky_sics['SIC19'] = funky_sics.SIC19.astype(str).str.zfill(8)
citylist = ['PHILADELPHIA', 'ANN ARBOR', 'COLUMBIA', 'BOSTON', 'SEATTLE', 'BERKELEY', 'SAN FRANCISCO', 'OAKLAND', 'SANTA FE']
statelist = ['PA', 'MI', 'SC', 'MA', 'WA', 'CA', 'CA', 'CA', 'NM']


funky_sics_places = pd.DataFrame()
for x,y in zip(citylist, statelist):
    df = funky_sics[(funky_sics['City'].str.rstrip() == x) & (funky_sics['State'].str.rstrip() == y)]
    funky_sics_places = pd.concat([funky_sics_places, df], ignore_index=True)

#%% WRITE TABLE TO EXCEL

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\reports\sic_check_20220506.xlsx') as writer:
    funky_sics_places.to_excel(writer)

#%% SEE IF ALL SICS IN QUESTION ARE IN DATASET

numslist = [*map(int,numslist)]
numslist = [*map(str,numslist)]
numslist = [n.zfill(8) for n in numslist]

sics_not_found = list(set(numslist) - set(funky_sics_places['SIC19']))


#%% ADD SIC DESCRIPTIONS/JANA COMMENTS; GET FREQS


sic_desc = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\sic_check_desc.txt', sep = '\t', dtype={"SICCode":str},  header=0, encoding_errors='replace', usecols=['SICCode', 'SICDescription', 'Jana Comment/potential grouping'])

out_df = pd.merge(funky_sics_places, sic_desc, left_on='SIC19', right_on='SICCode')

freqs = out_df['Jana Comment/potential grouping'].value_counts()

freqs.to_csv(r"C:\\Users\\stf45\\Documents\\NETS\\Processing/data_checks/sic_check_freqs.txt", sep="\t", header=header, mode='a', index=False)
