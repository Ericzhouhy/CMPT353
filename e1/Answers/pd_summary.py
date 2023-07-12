import pandas as pd

totals = pd.read_csv("C:/Users/ericz/Desktop/cmpt353/e1/totals.csv").set_index(keys=['name'])
counts = pd.read_csv("C:/Users/ericz/Desktop/cmpt353/e1/counts.csv").set_index(keys=['name'])

year_total = totals.sum(axis=1)
lowest_year_total_index = year_total.idxmin()

all_locations_month_total = totals.sum(axis=0)
all_locations_observe_count = counts.sum(axis=0)
all_locations_average_precipitation = all_locations_month_total/all_locations_observe_count

counts_total = counts.sum(axis=1)
city_average_presi = year_total / counts_total

#Answers:
print("City with lowest total precipitation: \n",lowest_year_total_index) #Answer for question 1
print("Average precipitation in each month: \n",all_locations_average_precipitation) #Answer for question 2
print("Average precipitation in each city: \n",city_average_presi) #Answer for question 3