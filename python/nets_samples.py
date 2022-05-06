# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 09:21:21 2022

@author: stf45
"""

import pandas as pd


# SAMPLE FILES

address_first_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\address_first_sample.txt', sep = '\t', dtype=object,  header=0)
company_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\company_sample.txt', sep = '\t', dtype=object,  header=0)
emp_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_sample.txt', sep = '\t', dtype=object,  header=0)
hqs_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\hqs_sample.txt', sep = '\t', dtype=object,  header=0)
hq_company_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\hq_company_sample.txt', sep = '\t', dtype=object,  header=0)
misc_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\misc_sample.txt', sep = '\t', dtype=object,  header=0)
move_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\move_sample.txt', sep = '\t', dtype=object,  header=0)
move_summary_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\move_summary_sample.txt', sep = '\t', dtype=object,  header=0)
sales_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_sample.txt', sep = '\t', dtype=object,  header=0)
sic_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_sample.txt', sep = '\t', dtype=object,  header=0)


# SAMPLE INTERMEDIATES

classification_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\classification_sample.txt', sep = '\t', dtype=object,  header=0)
geocoding_1_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocoding_1_sample.txt', sep = '\t', dtype=object,  header=0)
geocoding_2_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\geocoding_2_sample.txt', sep = '\t', dtype=object,  header=0)
sic_long_filter_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sic_long_filter_sample.txt', sep = '\t', dtype=object,  header=0)
first_last_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\first_last_sample.txt', sep = '\t', dtype=object,  header=0)
emp_long_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\emp_long_sample.txt', sep = '\t', dtype=object,  header=0)
sales_long_sample = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\samples\sales_long_sample.txt', sep = '\t', dtype=object,  header=0)


# FIRST 100,000 ROWS OF FULL FILES

address_first = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_AddressFirst.txt', sep = '\t', dtype=object, header=0, nrows=100000)
company = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Company.txt', sep = '\t', dtype=object, header=0, nrows=100000)
emp = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Emp.txt', sep = '\t', dtype=object, header=0, nrows=100000)
hq_company = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_HQCompany.txt', sep = '\t', dtype=object, header=0, nrows=100000)
hqs = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_HQs.txt', sep = '\t', dtype=object, header=0, nrows=100000)
move = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Move.txt', sep = '\t', dtype=object, header=0, nrows=100000)
move_summary = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_MoveSummary.txt', sep = '\t', dtype=object, header=0, nrows=100000)
sales = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_Sales.txt', sep = '\t', dtype=object, header=0, nrows=100000)
sic = pd.read_csv(r'D:\NETS\NETS_2019\RawData\NETS2019_SIC.txt', sep = '\t', dtype=object, header=0, nrows=100000)


# FIRST 100,000 ROWS OF INTERMEDIATE FILES

# sic_long_filter = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\nets_intermediate\sic_long_filter.txt', sep = '\t', dtype=object,  header=0, nrows=100000)
# first_last = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\nets_intermediate\first_last.txt', sep = '\t', dtype=object,  header=0, nrows=100000)
# geocoding_1 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\nets_intermediates\geocoding_1.txt', sep = '\t', dtype=object,  header=0, nrows=100000)
# geocoding_2 = pd.read_csv(r'C:\Users\stf45\Documents\NETS\Processing\nets_intermediates\geocoding_2.txt', sep = '\t', dtype=object,  header=0, nrows=100000)


