import numpy as np

data = np.load('C:/Users/ericz/Desktop/cmpt353/e1/monthdata.npz')
totals = data['totals']
counts = data['counts']

year_total = totals.sum(axis=1)
lowest_year_total_index = year_total.argmin()

all_locations_month_total = totals.sum(axis=0)
all_locations_observe_count = counts.sum(axis=0)
all_locations_average_precipitation = all_locations_month_total/all_locations_observe_count

counts_total = counts.sum(axis=1)
city_average_presi = year_total / counts_total

reshaped_totals = totals.reshape(9,4,3)
reshaped_totals_sum = reshaped_totals.sum(axis=2)

#Answers:
print("Row with lowest total precipitation: \n",lowest_year_total_index) #Answer for question 1
print("Average precipitation in each month: \n",all_locations_average_precipitation) #Answer for question 2
print("Average precipitation in each city: \n",city_average_presi) #Answer for question 3
print("Quarterly precipitation totals: \n",reshaped_totals_sum) #Answer for question 4