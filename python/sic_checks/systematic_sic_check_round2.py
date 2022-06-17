# -*- coding: utf-8 -*-
"""
Created on Fri May 13 15:11:09 2022

@author: stf45


This script is used as a second round check after spot checking the output 
(systematic_sic_check_20220511.xlsx) from systematic_sic_check.py. The SIC list
has been reduced from the previous round.


Inputs:
    data_checks\sic_check_places.txt (subset 30 places)
    reduced list of SICs in question (spot check == Y in systematic_sic_check_20220511SM.xslx)

Outputs: C:\Users\stf45\Documents\NETS\Processing\
    reports\systematic_sic_check_round2_20220513.xlsx 
        sheet1: random samples of sics in systematic review areas(n=5 unless fewer than 5 records available)
        sheet2: sic freqs in places
        sheet3: sics freqs in random samples
        
        
DOCUMENTATION FROM PREVIOUS STEP (systematc_sic_check.py):
    
    This script is used to create a subset of the NETS data using SIC codes of interest existing in the year 2019.
    The result is an excel file with the subset of records for particular SIC codes
    in particular places outlined in "Business Data Categorization and Refinement for 
    Application in Longitudinal Neighborhood Health Research: a Methodology, p.274.
    Places were subsetted using city and state names (mid size and large cities) 
    as well as county fips codes (rural counties).
    The subset is further reduced by acquiring random samples of 5 records for each
    SIC code (or all records if a SIC has 5 or fewer instances) in each place (theoretical                                                                         
    max # of records per SIC code is 150). The excel file will 
    be used to search businesses on google maps to discover more details regarding the 
    relevance of the SIC codes in question to public health research. 
    
    Inputs: D:\NETS\NETS_2019\RawData\
        NETS2019_SIC.txt
        NETS2019_Company.txt
        NETS2019_Emp.txt
        NETS2019_Misc.txt
        NETS2019_Sales.txt
        sic_potential_adds.txt (a csv file containing SICs in question with official SIC descriptions
                       and comments made by Jana)

    Outputs: C:\Users\stf45\Documents\NETS\Processing\
        data_checks\sic_check.txt (all SICs in sic_check)
        data_checks\sic_check_places.txt (subset 30 places)
        reports\systematic_sic_check_20220511.xlsx 
            sheet1: random samples of sics in systematic review areas(n=5 unless fewer than 5 records available)
            sheet2: sic freqs
            sheet3: sics not found in these places
"""


import pandas as pd
import numpy as np

#%% NEW CONSOLIDATED LIST OF SICS TO CHECK

sics2 = '''01399906
01619904
07520203
07520204
07520300
07520301
07529901
17949901
17950000
17999926
39440300
39440301
39440302
39440304
39440305
39440308
39440309
39440310
39440313
39440314
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
41110400
41110401
41110402
41110403
41119901
41119902
41119903
41190000
41190100
41190101
41190102
41190103
41199901
41199902
41199903
41199905
41199906
41210000
41310000
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
47299901
50470306
50470307
50470308
50470309
50470310
62820000
62829901
62829902
62829903
62829904
62829905
70320100
70320101
70320102
70320300
70330000
70339901
70339902
70339903
70339904
80499901
80499903
80499912
80599902
80820000
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

siclist2 = sics2.splitlines()
siclist2 = [*map(int, siclist2)]

#%% SUBSET BY REDUCED LIST OF SIC CODES 

potential_sics_places =  pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\sic_check_places.txt', sep = '\t', dtype={"SIC19":str})
potential_sics_places = potential_sics_places[potential_sics_places['SIC19'].isin(siclist2)]

#%% GET SIC FREQS WITH SIC DESCRIPTIONS/JANA COMMENTS

sic_desc = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\data_checks\sic_potential_adds.txt', sep = '\t', dtype={"SICCode":str},  header=0, encoding_errors='replace', usecols=['SICCode', 'SICDescription', 'Jana Comment/potential grouping'])
out_df = pd.merge(potential_sics_places, sic_desc, left_on='SIC19', right_on='SICCode')

sic_freqs = out_df['SIC19'].value_counts()
sic_freqs = pd.DataFrame(sic_freqs).reset_index()
sic_freqs.columns = ['SIC19', 'sic_counts']

sic_freqs = pd.merge(sic_freqs, sic_desc, left_on='SIC19', right_on='SICCode').drop(columns=['SICCode'])

#%% CREATE NEW PLACE VARIABLE FOR GROUPBY IN NEXT STEP

countylist = [23017, 36121, 42119, 21221, 17123, 48067, 19067, 8037, 6015, 53041]
potential_sics_places['check_area'] = np.where(potential_sics_places['FipsCounty'].isin(countylist), potential_sics_places['FipsCounty'].astype(str), potential_sics_places['City'])
        
#%% SELECT RANDOM SAMPLES OF 5 BY EACH SIC19, GET COUNTS

random_sample = potential_sics_places.groupby(['check_area','SIC19']).sample(n=5, replace=True).drop_duplicates()
random_sample = pd.merge(random_sample, sic_desc, left_on='SIC19', right_on='SICCode').drop(columns=['SICCode'])
random_sample['Longitude'] = random_sample['Longitude']*-1

counts = random_sample.SIC19.value_counts()
counts = pd.DataFrame(counts).reset_index()
counts.columns = ['SIC19', 'sic_counts']
sample_sic_freqs = pd.merge(counts, sic_desc, left_on='SIC19', right_on='SICCode').drop(columns=['SICCode'])

#%% WRITE TABLE TO EXCEL

with pd.ExcelWriter(r'C:\Users\stf45\Documents\NETS\Processing\scratch\systematic_sic_check_round2_20220513.xlsx') as writer:
    random_sample.to_excel(writer, "sics in systematic review areas", index=False)
    sic_freqs.to_excel(writer, "sic freqs in places", index=False)
    sample_sic_freqs.to_excel(writer, "sic freqs in random samples", index=False)