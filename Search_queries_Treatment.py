# -*- coding: utf-8 -*-
"""
Created on Sat Nov 10 05:05:01 2018

@author: amishra_v
"""

#importing required libraries
import pandas as pd
#import pyodbc
from nltk import word_tokenize
import re
from nltk.corpus import stopwords
from collections import Counter,defaultdict
#import itertools
import os
import time
import sys
import numpy as np
import getpass
from sklearn.metrics import homogeneity_completeness_v_measure




from text_preprocessing  import *
from utility import parallelize
#%%
### Importing User defined modules
import spell_correct as sc

# spell corrected string
def spell_correct(query):
    query = Apostrophe_treat(query)
    correct_text = sc.query_corrector(query)
    return correct_text


#Preprocssing of the queries
def proc_q_mod(q):
	q=alphanum(str(q))
	u=wtok(units(q))
	q=rem_stop_words(q,["at"]+u)
	q=alpha(q)
	q=wtok(q)+u
	q=lemmat(q)
	#storts the list of words in a keyword
	q.sort()
	
	if len(q)==0:
		return ''
	if len(q[0])==0 :
		return ''
	else:
		return ' '.join(q)


## Correcting Spellings
def preprocess_data(data):
	t1 = time.time()
	print("Correcting spellings... Input Keywords:", data.shape[0])
	data['keyword_corrected'] = data.apply(lambda row: spell_correct(row['Keyword']).strip(), axis=1)
	
	print("Spelling correction done in",round ((time.time()-t1)/60,2),"minutes")
	print("Deleting pickles for spell Check")
	#del [sc.Cleaned_df_ngramed_list_merged, sc.Ngram_freq_dict, sc.Spell_correction_dict]
	### Cleaning Keywords
	t1 = time.time()
	print("Cleaning Keywords...")
	data['cleaned_keyword'] = data.apply(lambda row: proc_q_mod(row['keyword_corrected']), axis=1)
	
	print("Cleaning done in ",round ((time.time()-t1)/60,2),"minutes")
	## Removing nulls
	data=data[(data.cleaned_keyword.notnull()) & (data.cleaned_keyword != '')]
	print("Total rows after cleaning", data.shape[0])
	return data[['Rank','cleaned_keyword']]



#%%
def clean_dataset(data):
	### Adding a rank according to Keywords
	data['Rank'] = data.Keyword.rank(method='dense').astype(int)

	### Keeping on non duplicated keywords
	data_= data[['Rank','Keyword']].drop_duplicates(['Rank','Keyword'], keep='first')
	
	print("Taking only unique keywords for cleaning. Total unique Keywords", data_.shape[0])
	data=data.replace({'SKU': r'[^0-9]+'}, {'SKU': ' '}, regex=True)
	### Cleaning  Keywords
	keyword_clean = parallelize(data_,preprocess_data)
	print(keyword_clean.head(2))
	### Joining Keywords with the original data table--> This will restore the frequency of each keyword wrt Product category
	output = pd.merge(data, keyword_clean, how='inner',on ='Rank',suffixes=('', '_y'))
	return output[['Keyword','cleaned_keyword','SKU','action']]

