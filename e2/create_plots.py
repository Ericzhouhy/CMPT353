import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib; matplotlib.use('TkAgg')
import sys

filename1 = sys.argv[1]
filename2 = sys.argv[2]

data1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1,names=['lang', 'page', 'views', 'bytes'])
data2 = pd.read_csv(filename2, sep=' ', header=None, index_col=1,names=['lang', 'page', 'views', 'bytes'])

data_merged = pd.merge(data1,data2,on = 'page',sort=True,suffixes = ('1','2'))

sorted_data1 = data1.sort_values(by=['views'],ascending=False)['views']
sorted_data2 = data2.sort_values(by=['views'],ascending=False)['views']

#plot 1
plt.figure(figsize=(10, 5))
plt.subplot(1, 2, 1)
plt.plot(sorted_data1.values)
plt.title('Popularity Distribution')
plt.xlabel("Rank")
plt.ylabel("Views")

#plot 2
plt.subplot(1, 2, 2)
plt.scatter(data_merged['views1'], data_merged['views2'], color='b')
plt.xscale("log")
plt.yscale("log")
plt.title("Hourly Correlation")
plt.xlabel("Hour 1 views")
plt.ylabel("Hour 2 views")

plt.savefig('wikipedia.png')