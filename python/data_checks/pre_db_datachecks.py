# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 09:45:53 2023

@author: stf45
"""

import pandas as pd

###############################################################################
# FINAL
###############################################################################

#%% ADDRESS

address = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\Addresses20231027.txt', sep='\t', dtype={'GcZIP':str, 'GcZIP4':str})

address.dtypes

stats = address.describe()

check = address.loc[address['GcAddress'].isna()]

'''
notes:
GcZIP == 9999 (n=941)
GcAddress is na (n=72710)
'''

address['GcAddress'].str.len().max()

for col in address.columns:
    if type(col) == str:
        strmax = address[col].str.len().max()
        print(f"{col}: {strmax}")
    else: 
        pass
    
#%% BUSINESS INFO

business = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\BusinessInfo20231025.txt', sep='\t', usecols=['DunsYear'])

business_stats = business.describe()

#%% CLASSIFIED LONG

classifiedlong = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231024.txt', sep='\t')

# get # of records
rownum = pd.DataFrame([len(classifiedlong)], columns=['n'])
unique_dunsyears = classifiedlong['DunsYear'].nunique()
unique_dunsnums = classifiedlong['DunsYear'].str[:9].nunique()
unique_dunsyears_df = pd.DataFrame([unique_dunsyears], columns=['n'])
# get sum of each record's total category count, then:
#get unique values to show how many records were not categorized (0), categorized once (1), etc
catcounts = classifiedlong.groupby(['DunsYear']).count().reset_index().rename(columns={'BaseGroup':'catcount'})
unique_catcounts = pd.DataFrame(catcounts['catcount'].value_counts()).reset_index().rename(columns={'index':'BaseGroup_Frequency','catcount':'Count'})
# get subset of all records categorized in > 2 categories, show cats
triplecats = catcounts.loc[catcounts['catcount'] > 2]
# get sum of each category's total record count
cat_freqs = classifiedlong['BaseGroup'].value_counts().reset_index().rename(columns={'index':'BaseGroup','BaseGroup':'count'}).sort_values('BaseGroup')

print(f"{rownum['n']}\n{unique_dunsyears}")
##

with pd.ExcelWriter(r'D:\scratch/NETS_classified_long_report.xlsx') as writer:
    cat_freqs.to_excel(writer, sheet_name='Classified DYs Per Cat', index=False)
    unique_catcounts.to_excel(writer, sheet_name='Cats Per DY Uniques', index=False)    
    triplecats.to_excel(writer, sheet_name='List of DYs in >2 Cats', index=False)
    rownum.to_excel(writer, sheet_name='Number of Records', index=False)
    unique_dunsyears_df.to_excel(writer, sheet_name='Unique DunsYears', index=False)
    
    
del classifiedlong
del catcounts
del triplecats
del writer

#%% DUNS MOVE

dunsmove = pd.read_csv(r'D:\scratch\DunsMove20231201.txt', sep='\t', usecols=['DunsYear','DunsMove','Year','AddressID'])

unique_dunsyears = dunsmove['DunsYear'].nunique()
unique_dunsnums = dunsmove['DunsYear'].str[:9].nunique()
unique_dunsyears = dunsmove['AddressID'].nunique()
dunsmove_stats = dunsmove.describe()




#%% DUNS MOVE

dunsmove = pd.read_csv(r'D:\scratch\DunsMove20231201.txt', sep='\t', usecols=['DunsYear','DunsMove','Year','AddressID'])

unique_dunsyears = dunsmove['DunsYear'].nunique()
unique_dunsnums = dunsmove['DunsYear'].str[:9].nunique()
unique_addressids = dunsmove['AddressID'].nunique()
unique_dunsmoves = dunsmove['DunsMove'].nunique()
dunsmove_stats = dunsmove.describe()

#%% TRACT LEVEL MEASURES

tractlevel = pd.read_csv(r'D:\scratch\NETS_tr10measures20231206.txt', sep='\t', dtype={'tract10':str})

nrows = len(tractlevel)
nrows/33

###############################################################################
# INTERMEDIATE
###############################################################################

#%% CLASSIFICATION INPUT PYTHON
classinputpy = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classification_input_Python20231017.txt', sep='\t', usecols=['DunsYear', 'DunsNumber'])
classinputpy_stats = classinputpy.describe()
del classinputpy

#%% CLASSIFICATION INPUT SAS
classinputsas = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classification_input_SAS20231016.txt', sep='\t', usecols=['DunsYear', 'DunsNumber'])
classinputsas_stats = classinputsas.describe()
del classinputsas

#%% CLASSIFICATION INPUT 
classinput = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classification_input20231016.txt', sep='\t', usecols=['DunsYear', 'DunsNumber'])
classinput_stats = classinput.describe()
del classinput

#%% CLASSIFIED PYTHON
classpy = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classified_python20231024.txt', sep='\t', usecols=['DunsYear'])
classpy_stats = classpy.describe()
del classpy

#%% CLASSIFIED SAS
classsas = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\classified_SAS20231018.txt', sep='\t', usecols=['DunsYear'])
classsas_stats = classsas.describe()
del classsas

#%% DUNSMOVE 1
dunsmove1 = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\dunsmove_1_20231027.txt', sep='\t', usecols=['DunsNumber', 'DunsMove'])
dunsmove1_stats = dunsmove1.describe()
del dunsmove1

#%% GEOCODING INPUT 1
geo1 = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\geocoding_1_20231027.txt', sep='\t', usecols=['DunsNumber', 'DunsMove'])
geo1_stats = geo1.describe()
del geo1

#%% GEOCODING INPUT 2
geo2 = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\geocoding_2_20231027.txt', sep='\t', usecols=['DunsNumber', 'DunsMove'])
geo2_stats = geo2.describe()
del geo2