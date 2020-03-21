
import numpy as np
import time
import pandas as pd
from multiprocessing import Pool,cpu_count

	
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

