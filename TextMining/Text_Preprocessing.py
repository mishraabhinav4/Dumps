# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 04:41:02 2018

@author: amishra_v
"""
import pandas as pd
import numpy as np
from nltk.data import load
import re
import csv
import nltk
from multiprocessing import cpu_count,Pool
from nltk import word_tokenize,ngrams
import itertools
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import warnings
warnings.filterwarnings('ignore')


#preparing lemmatizer
lemmatizer = WordNetLemmatizer()
tagdict = load('help/tagsets/upenn_tagset.pickle')
tags=list(tagdict.keys())
# listing pos tags for lemmatizing
lem_pos={}
for tag in tags:
    val=tag.lower()[0]
    if val in "nrv":
        lem_pos[tag]=val
    elif val in "j":
        lem_pos[tag]='a'




#reading stop words for each attribute
sws=pd.read_csv("Parameter_Files/stopwords.csv")
sws.fillna(value='', inplace=True)
sws=dict(sws)

#input customized cleaning parameters
col_custom=dict(pd.read_csv("Parameter_Files/Data_preprocessing_functions.csv"))
for attribute in sws.keys():
	sws[attribute]=[sw  for sw in sws[attribute] if len(sw)>0]

#creating a dictionary with keys as attribute types and text preprocessing functions as values of the dictionary values
cols_22=list(col_custom.keys())
for attribute in cols_22:
    for serial,fun in enumerate(col_custom[attribute]):
        if " sw" in str(fun):
            col_custom[attribute][serial]=(re.findall("(\w+)\s",fun)[0],list(sws[attribute]))
        elif " " in str(fun):
            val=fun.split(" ")
            if val[1].isnumeric():
                val[1]=int(val[1])
            col_custom[attribute][serial]=(val[0],val[1])
        if fun=='0':
            col_custom[attribute]=col_custom[attribute][:serial]

#intra-deletion
#destination or super set is the attribute from which the substring will be removed
#source or subset is the attribute which is being searched in super set attributes to be removed
dest_sources=pd.read_csv("Parameter_Files/Inter_column_deletion_rules.csv")#loading csv with destination and sources list for intra-deletion
#creating a dictionary out of the above parameter
d_s={}#d-destination & s-source
for index,row in dest_sources.iterrows():
    temp=row[1].split(",")
    temp=[i.split("-") for i in temp]
    tempo={}
    for i in temp:
        if len(i)==2:
            split_value=i[1].split("/")
            tempo[i[0]]=[int(x) for x in split_value]
        else:tempo[i[0]]=[]
    d_s[row[0]]=tempo
            
#importing the attribute weights to prioritize 
with open('Parameter_Files/Attribute_weight.csv', mode='r') as infile:
    reader = csv.reader(infile)
    weight_dict = dict((rows[0],float(rows[1])) for rows in reader if rows[0]!='attribute')
import pandas as pd
import numpy as np
from nltk.data import load
import re
import csv
import nltk
from multiprocessing import cpu_count,Pool
from nltk import word_tokenize,ngrams
import itertools
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import warnings
warnings.filterwarnings('ignore')


#preparing lemmatizer
lemmatizer = WordNetLemmatizer()
tagdict = load('help/tagsets/upenn_tagset.pickle')
tags=list(tagdict.keys())
# listing pos tags for lemmatizing
lem_pos={}
for tag in tags:
    val=tag.lower()[0]
    if val in "nrv":
        lem_pos[tag]=val
    elif val in "j":
        lem_pos[tag]='a'



#remove extra spaces from a string
def trim(stri):
    """Removes extra spaces from a string.
    Args:
		stri: input text
	returns:
		returns the text with no extra 
		
	>>> import text_preprocessing as tp
	>>> tp.trim('dog food   gluten free  ')
	'dog food gluten free'
    """
    stri=re.sub(r"^\s*|\s{2,}|\s*$"," ",str(stri))     
    return stri.strip()

#break strings based on defined separators for different columns
def breakit(stri,sep):
    """Break strings based on defined separators for different columns
    Args:
		stri: input text
		sep: separator of the text
	returns:
		returns the list containing split parts of the input text 
		
	>>> import text_preprocessing as tp
	>>> tp.breakit('small breed;large breed;medium breed',';')
	['small breed', 'large breed', 'medium breed']
    """
    return stri.split(sep)


#clean strings to keep only alphanumeric characters
def alphanum(stri):
    """clean strings to keep only alphanumeric characters
    Args:
		stri: input text
	returns:
		returns the text containing only alpha-numeric characters 
		
	>>> import text_preprocessing as tp
	>>> tp.alphanum('small breed;large breed;medium breed;24 kcal')
	'small breed large breed medium breed 24 kcal'
    """
    return re.sub(r'[^a-z0-9\s]',r' ', stri.lower())

#Removes all occurences of ' with no replacement
def Apostrophe_treat(stri):
    """Removes all occurences of ' with no replacement
    Args:
		stri: input text
	returns:
		returns the text with no '
		
	>>> import text_preprocessing as tp
	>>> tp.Apostrophe_treat("dog's tail")
	'dogs tail'
    """
    return trim(re.sub('[\']+', '',str(stri)).strip())
	

#clean strings to keep only english alphabets
def alpha(stri):
    """clean strings to keep only alphabet characters
    Args:
		stri: input text
	returns:
		returns the text containing only alphabet characters 
		
	>>> import text_preprocessing as tp
	>>> tp.alpha('small breed;large breed;medium breed;24 kcal')
	'small breed large breed medium breed    kcal'
    """
    return re.sub(r'[^a-z\s]',r' ', stri.lower())

#listing the words to be excluded from columns having units -Size,sizestandard and caloriccontent
rej=pd.read_csv('Parameter_Files/units_reject_list.csv')['rej']

#cleaning strings to remove any numeric character and measuremnt units 
def nounits(stri,flag_list=1):
    """remove units, counts or any quantitative words
    Args:
		stri: input text
	returns:
		returns the text without any word which is quantitative or a unit 
		
	In [51]: nounits('dry cat food 2lb')

	Out[51]: 'dry cat food  '

	In [52]: nounits('Aquarium-led-lights 24 inch')

	Out[52]: 'Aquarium-led-lights  '

	In [53]: nounits('dry cat food 2lb')

	Out[53]: 'dry cat food  '
    """
    return re.sub(r'(\b[0-9]+\.?[0-9]*\s*[-:]?\s*[a-z]{1,5}/?[a-z]{0,5})', " ",stri)

#extracting measurement units(eg.  ) from select attributes
def units(stri):
    """find all units, counts or any quantitative words and appends at the end of the text and removes all the numeric data
    Args:
		stri: input text
	returns:
		returns the text without any numbers and all units placed at the end of the text 
		
	In [48]: units('dry cat food 2lb')

	Out[48]: 'dry cat food   lb'

	In [49]: units('Aquarium-led-lights 24 inch')

	Out[49]: 'Aquarium-led-lights   inch'

	In [50]: units('pedigree 22 oz case')

	Out[50]: 'pedigree   case oz'
	
    """
    fun=re.findall(r'\b[0-9]+\.?[0-9]*\s*[-:]?\s*([a-z]{1,5}/?[a-z]{0,5})', stri)
    fun=list(set(fun) - set(rej))
    stri=nounits(stri)
    return stri+' '+' '.join(fun)

#removing stop words
stop_coll=stopwords.words('english')
def rem_stop_words(stri,extras):
    """removes all the stop words
    Args:
		stri: input text
	returns:
		returns the text without the stop words
	>>> import text_preprocessing as tp
	>>> tp.rem_stop_words('4 kcal of x and 5 kcal of y',['at'])
	'4 kcal x 5 kcal'
    """
    for stopword in stop_coll+extras:
        stri=re.sub(r"\b"+stopword+r"\b"," ",stri)
    return trim(stri)
	
#tokenization of string
def wtok(stri):
    return sorted(word_tokenize(stri))
	
#lemmatizing the words
def lemmat(words_list):
    lemmatized_list=[]
    for word in   words_list:
        lemmatized_list.append(lemmatizer.lemmatize(word))
    return lemmatized_list


#to create list of skip-grams 
def word_comb(words_list,n):
    combinations=[]
    for j in range(n,0,-1):
        combinations.extend(list(itertools.combinations(words_list,j)))
    #distinct.sort(reverse=True,key=len)
    return combinations

#to create list of n-grams
def ngrms(words_list,n):
	consec_words_combinations=[]
	for j in range(n,0,-1):
		consec_words_combinations.extend(list(ngrams(words_list,j)))
	return consec_words_combinations,len(words_list)

#Customized cleaning of data in each column 
#defining function which takes the text and the attribute as input and gives out n-grams,original text and n-gram lengths as output
def process_text(text,key):
    processing_functions=col_custom[key]
    for fun in processing_functions[1:]:#looping through list of functions for the attribute(key)
        if len(fun)!=2:
            command=f"{fun}(text)"
            text=eval(command)#apply the cleaning functions on the text
        else:#for functions having 2 arguement according to the file 'data_preprocessing_functions.csv'
            command=f"{fun[0]}(text,fun[1])"
            text=eval(command)
    ngrams=[]
    ngram_len=[]
    for stri in text[0]:
        ngram_len.append(len(stri))
        ngrams.append(" ".join(sorted(stri)))
    return ngrams,text[1],ngram_len

#process the keywords
def process_keyword(keyword):
    keyword=wtok(keyword)#word tokenization
	#generating the skipgrams out of the word tokens created
    keyword_skipgrams=word_comb(keyword,4)
    skipgrams=[]
    for stri in keyword_skipgrams:
        skipgrams.append(" ".join(sorted(stri)))#appending the skipgrams to a list 
    if len(skipgrams[0])==0:return [],[]
    return skipgrams


