# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 10:27:41 2022

@author: stf45
"""

#%%
import pandas as pd

#%% READ IN CSVS

jquinn = pd.read_csv(r'D:\NETS\NETS_2020\geocoding\nets_tall_priority_xy20220916.csv', usecols={'behid', 'accu', 'uhc_x', 'uhc_y'}, header=0)
dwalls = pd.read_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/geocoding_2xy.txt", sep='\t', usecols={'DunsMove', 'DunsNumber', 'Latitude', 'Longitude'}, dtype={'DunsNumber': str},  header=0)

#%% SUBSET FOR PHILLY, ADD DUNSNUMBER COLUMN TO PHILA_QUINN

# subset jquinn for XYs within phila bounding coords: (-75.280266,39.867004) 	(-74.955763,40.137992)
phila_quinn = jquinn.loc[(jquinn['uhc_x'] >= -75.280266) & (jquinn['uhc_x'] <= -74.955763) & (jquinn['uhc_y'] >= 39.867004) & (jquinn['uhc_y'] <= 40.137992)]

# subset dwalls for behids (aka 'DunsMove') matching those of jquinn
phila_dwalls = dwalls[dwalls['DunsMove'].isin(phila_quinn['behid'])]

# add dunsnumber column to phila_quinn
phila_quinn['DunsNumber'] = phila_quinn['behid'].astype(str).str[2:]

# check for any nulls in df
phila_quinn.isnull().values.any()


#%% COMPARE DUNSNUMBERS 

# check how many locs per dunsnumber in phila_quinn, add as column to phila_quinn. export to csv
phila_quinn['loc_count'] = phila_quinn.groupby(['DunsNumber'])['behid'].transform('count')
phila_quinn.to_csv(r'D:\NETS\NETS_2020\geocoding\data_checks\NETS2020quinn_phila.csv', header=True, index=False)

# get unique value counts of DunsNumber col in phila_quinn
addcount = phila_quinn['DunsNumber'].value_counts()
pq_dunscount = pd.DataFrame(addcount).reset_index()
pq_dunscount.columns = ['DunsNumber', 'count2020']

# check how many locs per dunsnumber in phila_dwalls, add as column to phila_dwalls. export to csv
phila_dwalls['loc_count'] = phila_dwalls.groupby(['DunsNumber'])['DunsMove'].transform('count')
phila_dwalls.to_csv(r'D:\NETS\NETS_2020\geocoding\data_checks\NETS2019dwalls_phila.csv', header=True, index=False)

# get unique value counts of DunsNumber col in phila_dwalls
addcount = phila_dwalls['DunsNumber'].value_counts()
pd_dunscount = pd.DataFrame(addcount).reset_index()
pd_dunscount.columns = ['DunsNumber', 'count2019']

# merge dfs, keeping unmatched records from quinn
compare = pq_dunscount.merge(pd_dunscount, on='DunsNumber', how='left')
diff = compare.loc[compare['count2019'] != compare['count2020']]
diff.to_csv(r'D:\NETS\NETS_2020\geocoding\data_checks\quinn_duns_unmatched.csv', header=True, index=False)

# check how many non null values in count2019 column of diff
notnull = diff['count2019'].count()

# merge dfs, keeping unmatched records from walls
compare2 = pq_dunscount.merge(pd_dunscount, on='DunsNumber', how='right')
diff2 = compare2.loc[compare2['count2019'] != compare2['count2020']]
diff2.to_csv(r'D:\NETS\NETS_2020\geocoding\data_checks\dwalls_duns_unmatched.csv', header=True, index=False)

retroadd = diff.loc[(diff['count2020']>1) & (diff['count2019'].isnull())]
retroadd.to_csv(r'D:\NETS\NETS_2020\geocoding\data_checks\retroadd.csv', header=True, index=False)


#%% DATACHECKS ON DUNS FROM DISTANT XYS

address = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_AddressFirst.txt', sep = '\t', dtype=object, encoding='latin-1',  header=0)

dunscheck = address.loc[address['DunsNumber']=='018591094']
