# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 04:46:30 2018

@author: Abhinav Mishra
"""

#Loading and dumping of files containing large size variables
import pickle
import numpy as np
import time
import pandas as pd
from multiprocessing import Pool,cpu_count

#save python variable as a pickle file
def save_it(data,pathname):
    pickle.dump( data, open( f"{pathname}.p", "wb" ) )

#load a pickle file as python variable
def pickleit(pathname):
    file = pickle.load( open( f"{pathname}.p", "rb" ) )
    return file

#To create fiscal year and month
def fiscal_year_new(year, month):
       if month <= 6:
            return year,month+6
       else:
            return year+1,month-6

def missing_val_analysis(data):
    missing_vals  = data.isnull().sum()
    missing_vals_prop = missing_vals*100/ len(data)
    missing_df=pd.concat([missing_vals[missing_vals != 0], missing_vals_prop[missing_vals_prop != 0] ], axis = 1)
    missing_df.columns = ['Total Values', 'Proportion']
    return missing_df

# convert dataframe columns to numeric
def convert_to_numeric(data, column_list):
    for col in column_list:
        data[col]=pd.to_numeric(data[col])
    return data

#replace space with underscore in column names
def rename_col(stri):
    stri=re.sub(r" ","_",stri.strip())
    return stri
	
def Flag_Treat(Flag):
    Flag=trim(re.sub(r'[^a-z\s]','',Flag.lower()))
    return Flag


# In[3]:


#Creating a month name to month number mapping (eg. Jan - 1, Feb - 2 ...etc)

months=dict((v,k) for k,v in enumerate(calendar.month_abbr))
month_df=pd.DataFrame({'Calendar_Month_Name':list(months.keys()),'Calendar_Month':list(months.values())})

#month number to name
months=dict((k,v) for k,v in enumerate(calendar.month_abbr))
def prev_ym(y,m,n=3):
    if m-n<=0:
        m=m-n+12
        y=y-1
    else:m=m-n
    Cal_YM=y*100+m
    Cal_YMS=str(y)+"-"+str(months[m])
    return Cal_YM , Cal_YMS

	

# In[4]:

#Load raw csv file automatically using filename

def Load_Raw_Data(file_name):
    file_name='Raw_input/*'+file_name+'*.csv'
    somefile_path = glob.glob(file_name) #glob takes only one argument
    df = pd.read_csv(somefile_path[0])
    #print(somefile_path)
    return df


# In[3]:



	
#parallelize the processes by distributing work among various cores
def parallelize(data,func,parts=cpu_count()):
	if data.shape[0]<parts:parts=data.shape[0]#handling the case when number of rows in the table are less than the number of available cores
	chunk_size=int(data.shape[0]/parts)
	data_split = np.array_split(data, parts)
	pool = Pool(parts)
	print(f"creating and will process {parts} new fragments of {chunk_size} rows")
	parallel_out=pd.concat(pool.map(func, data_split))
	print(f"processed the  {parts} fragments")
	pool.close()
	pool.join()
	return parallel_out
	
#parallelizing the processes by assigning fixed number of rows to a core(handling core capacity)
def parallelize_fixed(data,func):
	lim=1000#max rows to be sent to a core
	max_parts=cpu_count()
	max_size=data.shape[0]/max_parts
	if max_size>lim:#if rows per division is more than the core limit set then divide the dataframe further down
		parts=int(max_size/lim)
		print(f"creating and will process {parts} new fragments of {lim} rows")
		data_split=np.array_split(data,parts)
		parallel_out=pd.DataFrame()
		for i in range(0,parts,max_parts):
			data_=data_split[i:i+max_parts]	
			part_cnt=len(data_split)
			pool = Pool(part_cnt)
			out=pd.concat(pool.map(func, data_))
			pool.close()
			pool.join()
			parallel_out=parallel_out.append(out)
			print(f"processed the {parts_cnt} fragments")
			del out
		del data_split
	else: return parallelize(data,func)
	return parallel_out

