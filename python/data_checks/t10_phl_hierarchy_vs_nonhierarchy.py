# -*- coding: utf-8 -*-
"""
Created on Mon May  6 11:05:13 2024

@author: stf45
"""

import pandas as pd

#%% READ IN FILES

phl2010 = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Checks\BEDDN_t10_phl2010_20240216.txt', sep='\t')
phl2022 = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Checks\BEDDN_t10_phl2022_20240216.txt', sep='\t')
phl2010hier = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Checks\BEDDN_t10_phl2010_hier20240506.txt', sep='\t')
phl2022hier = pd.read_csv(r'Z:\UHC_Data\NETS_UHC\NETS2022\Data\Checks\BEDDN_t10_phl2022_hier20240506.txt', sep='\t')

#%% COMPARE

# this creates a dataframe for comparison of hierarchy vs non-hierarchy versions
#for each year. respective columns from each df are aligned next to each other 
#with an additional column index ('regular' or 'hierarchy'). values provided are
#actual values of each df. cell value is nan where there is no difference between 
#dfs. entire tracts with no differences are not shown in compare dfs.
compare2010 = (phl2010
               .compare(phl2010hier)
               .rename(columns={'self': 'regular', 'other': 'hierarchy'}, level=1)
               )
compare2022 = (phl2022
               .compare(phl2022hier)
               .rename(columns={'self': 'regular', 'other': 'hierarchy'}, level=1)
               )

# why are there more differences in 2022 than in 2010?

#%% 2010: ARE ALL HIERARCHY VALUES LOWER THAN REGULAR?

# get measure columns (first level of index)
measure_cols = compare2010.columns.levels[0]
# for each measure column, subtract hierarchy version from regular version
diff2010 = pd.concat([(compare2010[(col,'regular')] - compare2010[(col,'hierarchy')]) for col in measure_cols], axis=1)
# rename columns of diff df
diff2010.columns = measure_cols

# are there any negative values? 
(diff2010 < 0).values.any() # as of 05/06/2024: no

#%% 2022: ARE ALL HIERARCHY VALUES LOWER THAN REGULAR?

# get measure columns (first level of index)
measure_cols = compare2022.columns.levels[0]
# for each measure column, subtract hierarchy version from regular version
diff2022 = pd.concat([(compare2022[(col,'regular')] - compare2022[(col,'hierarchy')]) for col in measure_cols], axis=1)
# rename columns of diff df
diff2022.columns = measure_cols

# are there any negative values? 
(diff2022 < 0).values.any() # as of 05/06/2024: no



