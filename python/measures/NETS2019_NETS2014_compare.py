# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 11:48:35 2023

@author: stf45
"""
#%%
import pandas as pd
from sas7bdat import SAS7BDAT
import seaborn as sns
from matplotlib import pyplot as plt

#%% LOAD NETS2019 PHILLY DATA 2000 & 2010

philly = pd.read_csv(r'\\files.drexel.edu\colleges\SOPH\Shared\UHC\Projects\NETS\Data\NETS2019_Python\NETS_tr10_measure_philly20230731.txt', sep='\t')

philly00 = philly.loc[(philly['Year'] == 2000)]
philly10 = philly.loc[philly['Year'] == 2010]

#%% LOAD PEDIATRIC BIG DATA NETS2014 PHILLY 2000 & 2010

with SAS7BDAT(r'Z:\UHC_Data\NETS_UHC\SAS\panjdemdnetsv5_uhc_den_122018.sas7bdat') as file:
    pbd = file.to_data_frame()

pbd = pbd.loc[pbd['geoid10'].str[:5] == '42101']

pbd00 = pbd[['geoid10',
             'net_fsa_uhc_den_2000',
             'net_rsa_uhc_den_2000',
             'net_wal_uhc_den_2000',
             'net_fin_uhc_den_2000',
             'net_pos_uhc_den_2000',
             'net_lib_uhc_den_2000',
             'net_beu_uhc_den_2000',
             'net_lau_uhc_den_2000',
             'net_fsr_uhc_den_2000',
             'net_sid_uhc_den_2000',
             'net_ngt_uhc_den_2000',
             'net_tan_uhc_den_2000',
             'net_cmp_uhc_den_2000',
             'net_psc_uhc_den_2000',
             'net_dra_uhc_den_2000',
             'net_cmu_uhc_den_2000',
             'net_cvp_uhc_den_2000',
             'net_dcr_uhc_den_2000',
             'net_scl_uhc_den_2000',
             'net_liq_uhc_den_2000',
             'net_bar_uhc_den_2000',
             'net_sma_uhc_den_2000',
             'net_cna_uhc_den_2000',
             'net_ffa_uhc_den_2000',
             'net_usr_uhc_den_2000',
             'net_aur_uhc_den_2000',
             'net_hsr_uhc_den_2000']]

pbd10 = pbd[['geoid10',
             'net_fsa_uhc_den_2010',
             'net_rsa_uhc_den_2010',
             'net_wal_uhc_den_2010',
             'net_fin_uhc_den_2010',
             'net_pos_uhc_den_2010',
             'net_lib_uhc_den_2010',
             'net_beu_uhc_den_2010',
             'net_lau_uhc_den_2010',
             'net_fsr_uhc_den_2010',
             'net_sid_uhc_den_2010',
             'net_ngt_uhc_den_2010',
             'net_tan_uhc_den_2010',
             'net_cmp_uhc_den_2010',
             'net_psc_uhc_den_2010',
             'net_dra_uhc_den_2010',
             'net_cmu_uhc_den_2010',
             'net_cvp_uhc_den_2010',
             'net_dcr_uhc_den_2010',
             'net_scl_uhc_den_2010',
             'net_liq_uhc_den_2010',
             'net_bar_uhc_den_2010',
             'net_sma_uhc_den_2010',
             'net_cna_uhc_den_2010',
             'net_ffa_uhc_den_2010',
             'net_usr_uhc_den_2010',
             'net_aur_uhc_den_2010',
             'net_hsr_uhc_den_2010']]

#%% REORDER COLUMN FUNCTION

# reorder columns alphabetically
def colsort(df, nosort=None):
    """
    
    Parameters
    ----------
    df : Dataframe
        DataFrame whose columns need resorting in alphabetical order.
    nosort : List; optional
        List of columns not to sort. These columns will be placed back into df
        in order of list. The default is None.

    Returns
    -------
    df : Dataframe
        Sorted Dataframe.

    """
    cols = list(df.columns)
    [cols.remove(col) for col in nosort]
    cols.sort()
    [cols.insert(0, col) for col in nosort]
    df = df[cols]
    return df

#%% reorder columns

philly00 = colsort(philly00, nosort=['tract10','Year'])
philly10 = colsort(philly10, nosort=['tract10','Year'])
pbd00 = colsort(pbd00, nosort=['geoid10'])
pbd10 = colsort(pbd10, nosort=['geoid10'])

#%% PLOT HISTOGRAMS AND SAVE

############ AUR ###############
#%% NETS2019 2000 "AUR" category
p1 = sns.histplot(data=philly00, x='t10_net_aur_d', color='green')
p1.set(xlabel='Density (units/sqkm)', title='NETS2019 AUR TRACT DENSITY IN 2000')
pic = p1.get_figure()
pic.savefig(r"Downloads\NETS2019_AUR2000_hist.png", format="png", dpi=300)
#%% NETS2014 2000 "AUR" category
p2 = sns.histplot(data=pbd00, x='net_aur_uhc_den_2000', color='green')
p2.set(xlabel='Density (units/sqkm)', title='NETS2014 AUR TRACT DENSITY IN 2000')
pic = p2.get_figure()
pic.savefig(r"Downloads\NETS2014_AUR2000_hist.png", format="png", dpi=300)
#%% NETS2019 2010 "AUR" category
p3 = sns.histplot(data=philly10, x='t10_net_aur_d')
p3.set(xlabel='Density (units/sqkm)', title='NETS2019 AUR TRACT DENSITY IN 2010')
pic = p3.get_figure()
pic.savefig(r"Downloads\NETS2019_AUR2010_hist.png", format="png", dpi=300)
#%% NETS2014 2010 "AUR" category
p4 = sns.histplot(data=pbd10, x='net_aur_uhc_den_2010')
p4.set(xlabel='Density (units/sqkm)', title='NETS2014 AUR TRACT DENSITY IN 2010')
pic = p4.get_figure()
pic.savefig(r"Downloads\NETS2014_AUR2010_hist.png", format="png", dpi=300)


############ HSR ###############
#%% NETS2019 2000 "HSR" category
p1 = sns.histplot(data=philly00, x='t10_net_hsr_d', color='green')
p1.set(xlabel='Density (units/sqkm)', title='NETS2019 HSR TRACT DENSITY IN 2000')
pic = p1.get_figure()
pic.savefig(r"Downloads\NETS2019_HSR2000_hist.png", format="png", dpi=300)
#%% NETS2014 2000 "HSR" category
p2 = sns.histplot(data=pbd00, x='net_hsr_uhc_den_2000', color='green')
p2.set(xlabel='Density (units/sqkm)', title='NETS2014 HSR TRACT DENSITY IN 2000')
pic = p2.get_figure()
pic.savefig(r"Downloads\NETS2014_HSR2000_hist.png", format="png", dpi=300)
#%% NETS2019 2010 "HSR" category
p3 = sns.histplot(data=philly10, x='t10_net_hsr_d')
p3.set(xlabel='Density (units/sqkm)', title='NETS2019 HSR TRACT DENSITY IN 2010')
pic = p3.get_figure()
pic.savefig(r"Downloads\NETS2019_HSR2010_hist.png", format="png", dpi=300)
#%% NETS2014 2010 "HSR" category
p4 = sns.histplot(data=pbd10, x='net_hsr_uhc_den_2010')
p4.set(xlabel='Density (units/sqkm)', title='NETS2014 HSR TRACT DENSITY IN 2010')
pic = p4.get_figure()
pic.savefig(r"Downloads\NETS2014_HSR2010_hist.png", format="png", dpi=300)


############ USR ###############
#%% NETS2019 2000 "USR" category
p1 = sns.histplot(data=philly00, x='t10_net_usr_d', color='green')
p1.set(xlabel='Density (units/sqkm)', title='NETS2019 USR TRACT DENSITY IN 2000')
pic = p1.get_figure()
pic.savefig(r"Downloads\NETS2019_USR2000_hist.png", format="png", dpi=300)
#%% NETS2014 2000 "USR" category
p2 = sns.histplot(data=pbd00, x='net_usr_uhc_den_2000', color='green')
p2.set(xlabel='Density (units/sqkm)', title='NETS2014 USR TRACT DENSITY IN 2000')
pic = p2.get_figure()
pic.savefig(r"Downloads\NETS2014_USR2000_hist.png", format="png", dpi=300)
#%% NETS2019 2010 "USR" category
p3 = sns.histplot(data=philly10, x='t10_net_usr_d')
p3.set(xlabel='Density (units/sqkm)', title='NETS2019 USR TRACT DENSITY IN 2010')
pic = p3.get_figure()
pic.savefig(r"Downloads\NETS2019_USR2010_hist.png", format="png", dpi=300)
#%% NETS2014 2010 "USR" category
p4 = sns.histplot(data=pbd10, x='net_usr_uhc_den_2010')
p4.set(xlabel='Density (units/sqkm)', title='NETS2014 USR TRACT DENSITY IN 2010')
pic = p4.get_figure()
pic.savefig(r"Downloads\NETS2014_USR2010_hist.png", format="png", dpi=300)


