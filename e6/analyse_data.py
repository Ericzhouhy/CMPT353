from ast import Index
import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import matplotlib.pyplot as plt

data = pd.read_csv("data.csv")
anova = stats.f_oneway(data['qs1'], data['qs2'], data['qs3'], data['qs4'], data['qs5'], data['merge1'], data['partition_sort'])
print("p-value from one way anova test = ", anova.pvalue)
if anova.pvalue <0.05:
    print("\nAs", anova.pvalue, " < .05 we may can conclude that there a difference in the mean of the samples.\n" )
else:
    print("\nAs", anova.pvalue, " > .05 we cannot conclude that there a difference in the mean of the samples.\n" )

qs1_mean = data.iloc[:,0].mean()    
qs2_mean = data.iloc[:,1].mean()
qs3_mean = data.iloc[:,2].mean()
qs4_mean = data.iloc[:,3].mean()
qs5_mean = data.iloc[:,4].mean()
mergesort_mean = data.iloc[:,5].mean()
partition_mean = data.iloc[:,6].mean()

print("Mean time of qs1 is: ",qs1_mean)
print("Mean time of qs2 is: ",qs2_mean)
print("Mean time of qs3 is: ",qs3_mean)
print("Mean time of qs4 is: ",qs4_mean)
print("Mean time of qs5 is: ",qs5_mean)
print("Mean time of mergesort is: ",mergesort_mean)
print("Mean time of partition is: ",partition_mean)