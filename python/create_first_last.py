# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 09:48:07 2023

@author: stf45
"""

#%% 
import pandas as pd
#%% READ IN AND ADJUST

# read in ['DunsNumber','FirstYear','LastYear'] columns and add +1 to all rows in FirstYear col
#(There are no SIC data for the first year of all records, so the given first year should not
#be included)
misc =  pd.read_csv(r"\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\RawData\NETS2019_Misc\NETS2019_Misc.txt", sep = '\t', header=0, usecols=['DunsNumber','FirstYear','LastYear'],encoding_errors='replace')
misc['FirstYear'] += 1

#%% WRITE TO CSV

misc.to_csv(r"C:\Users\stf45\Documents\NETS\Processing\scratch/first_last.txt", sep="\t", index=False)
