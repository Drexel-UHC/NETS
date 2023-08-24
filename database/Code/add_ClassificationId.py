# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 14:08:28 2023

@author: stf45
"""

import pandas as pd

df = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\ClassifiedLong20230526.txt', sep='\t')

df['ClassificationId'] = pd.Series(range(1,264008388))

df = df[['ClassificationId', 'DunsYear', 'BaseGroup']]

df.to_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\ClassifiedLong20230822.txt', sep='\t', index=False)

