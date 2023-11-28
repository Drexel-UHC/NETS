# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 14:08:28 2023

@author: stf45
"""

import pandas as pd

df = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231024.txt', sep='\t')

df['ClassificationId'] = pd.Series(range(1,len(df)+1))

df = df[['ClassificationId', 'DunsYear', 'BaseGroup']]

df.to_csv(r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231127.txt', sep='\t', index=False)

