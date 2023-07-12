import time
import pandas as pd
import numpy as np
from implementations import all_implementations

data = pd.DataFrame(index = range(500), columns=['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'])

for i in range(500):
    random_array = np.random.randint(0,1000,size = 500)
    for sort in all_implementations:
        st = time.time()
        res = sort(random_array)
        en = time.time()
        data.iloc[i][sort.__name__] = en - st

data.to_csv('data.csv', index=False)