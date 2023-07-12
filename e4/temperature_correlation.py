import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt

def distance(cities, stations):
    R = 6371; # Radius of the earth in km
    dLat = np.radians(np.subtract(cities['latitude'], stations['latitude']))
    dLon = np.radians(np.subtract(cities['longitude'], stations['longitude']))
    a = np.sin(dLat/2) **2 + np.cos(np.radians(stations['latitude'])) * np.cos(np.radians(cities['latitude'])) * np.sin(dLon/2) **2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)) 
    d = R * c * 1000
    return d

def best_tmax(city, stations):
    list = distance(city, stations)
    return stations.loc[list.idxmin()]['avg_tmax']

def main():
    
    stationsFile =sys.argv[1]
    citiesFile = sys.argv[2]
    outputFile = sys.argv[3]
    
    cities = pd.read_csv(citiesFile)
    cities = cities.dropna()
    cities['area'] = cities['area']/(1000*1000)#convert m^2 to km^2
    cities = cities[cities['area'] <= 10000] #remove city greater than 10000km^2
    cities['density'] = cities['population']/cities['area']

    stations = pd.read_json(stationsFile, lines=True)
    stations['avg_tmax'] = stations['avg_tmax']/10 # divide 10
    cities['max'] = cities.apply(best_tmax, stations = stations, axis = 1)
    
    plt.figure(figsize=(12,7))
    plt.scatter(cities['max'], cities['density'], alpha=0.5)
    plt.xlabel("Avg Max Temperature (\u00b0C)")
    plt.ylabel("Population Density (people/km\u00b2)")
    plt.title("Temperature vs Population Density")
    plt.savefig(outputFile)


main()