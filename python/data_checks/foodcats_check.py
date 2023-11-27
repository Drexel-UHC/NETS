# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 15:21:03 2023

@author: stf45
"""

import pandas as pd
import matplotlib.pyplot as plt
#%% LOAD AND REFORMAT DATA

desc = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\CategoryDescriptions20231023.txt', sep='\t')

# assign healthcare columns to usecols to subset beddn upon load
fooddesc = desc['Category'].loc[(desc['Domain'] == 'Food') & (desc['Type'] == 'Base Group')]  
foodcats = list(fooddesc)

# load NETS
classlong = pd.read_csv(r'D:\NETS\NETS_2022\ProcessedData\ClassifiedLong20231024.txt', sep='\t')

food = classlong.loc[classlong['BaseGroup'].isin(foodcats)]
food['Year'] = food['DunsYear'].str[-4:]

agg = food.groupby(['Year','BaseGroup']).count().reset_index().rename(columns={'DunsYear':'Count'})
agg = agg.sort_values(['BaseGroup','Year'])

#%% CREATE AND SAVE TIMELINE COUNT PLOTS FOR EACH FOOD CATEGORY

for cat in foodcats:
    subset = agg.loc[agg['BaseGroup'] == cat]
    plt.figure()
    x = subset['Year']
    y = subset['Count']
    plt.plot(x,y)
    plt.title(cat, fontsize=30)
    plt.xticks(range(0, len(subset)+1, 5))
    # plt.show()
    plt.savefig(fr'D:\scratch\NETS2022_{cat}_count1990_2022.jpg')
    plt.close()
    
#%% PIVOT AGG TO WIDE AND EXPORT TO EXCEL FILE

aggwide = agg.pivot(index='Year', columns='BaseGroup', values='Count')

with pd.ExcelWriter(r'D:\scratch\NETS2022_foodcategory_counts1990_2022.xlsx') as writer:
    aggwide.to_excel(writer)