#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import warnings
#import re
import os
from Utilities import *
warnings.filterwarnings("ignore")


import logging
logging.info("loading ADS for oultier detection and analysis")


# In[3]:


#loading ADS and column list on which outlier treatment is to be performed 

ads=pd.read_csv("Output/Filename.csv")
outlier_cols=pd.read_csv("Supporting_Files/outlier_input_columns.csv")


# In[4]:


outlier_cols=list(outlier_cols['Input_cols'])


# In[5]:

factor=2




#finding Q1 and Q3 values for all required columns at the level of product flag and product type

Q1 = ads[['Product_flag','Product_Category']+outlier_cols].groupby(['Product_flag','Product_Category']).quantile(0.25).reset_index()
Q3 = ads[['Product_flag','Product_Category']+outlier_cols].groupby(['Product_flag','Product_Category']).quantile(0.75).reset_index()


# In[7]:


#merge the values to ads

ads=pd.merge(ads,Q1, how='left', on=['Product_flag','Product_Category'],suffixes=('','_q1'))
ads=pd.merge(ads,Q3, how='left', on=['Product_flag','Product_Category'],suffixes=('','_q3'))


# In[8]:


#create flag columns for all the required columns for which outlier treatment has to be performed
ads['Outlier']=0
for col in outlier_cols:
    ads['IQR']=ads[col+"_q3"]-ads[col+"_q1"]
    ads[col+"_outlier_flag"]=np.where((ads[col]>(ads[col+"_q3"]+factor*ads['IQR'])) | (ads[col]<(ads[col+"_q1"]-factor*ads['IQR'])),1,0)
    ads['Outlier']=ads['Outlier']+ads[col+"_outlier_flag"]
    flag_percentage=ads.groupby(['Product_flag','Product_Category',col+"_outlier_flag"]).size().reset_index(name='count')
    flag_percentage['tot']=flag_percentage.groupby(['Product_flag','Product_Category'])['count'].transform('sum')
    flag_percentage['tot_percent']=flag_percentage['count']*100/flag_percentage['tot']
    flag_percentage.to_csv("Outlier_Report/percent"+col+".csv",index=False)
ads['Outlier']=np.where(ads['Outlier']>0,'Yes','No')


# In[9]:


#remove unwanted/intermediate columns from ads
drop_cols = [col for col in ads.columns if '_q1' in col] +  [col for col in ads.columns if '_q3' in col] + ['IQR']
ads=ads.drop(drop_cols,axis=1)


# In[10]:


#remove unwanted/intermediate columns from ads
drop_cols = ads.filter(regex='_q1|_q3|_outlier_flag|IQR').columns
ads=ads.drop(drop_cols,axis=1)


# In[11]:


save_it(ads,'Output/new_file.csv')
ads.to_csv('Output/new_file.csv',index=False)



