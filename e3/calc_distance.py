import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from pykalman import KalmanFilter
import sys

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.7f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')

def get_data(file):
    parse_result = ET.parse(file)
    lat_data = []
    lon_data = []

    for item in parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt'):
        lat_data.append(float(item.attrib['lat']))
        lon_data.append(float(item.attrib['lon']))

    data = pd.DataFrame()
    data['lat'] = lat_data
    data['lon'] = lon_data
    return data

def distance(data): 
    shifted = data.shift(periods=1)
    R = 6371000; # Radius of the earth
    
    dLat = np.radians(np.subtract(shifted['lat'], data['lat']))
    dLon = np.radians(np.subtract(shifted['lon'], data['lon']))
    a = np.sin(dLat/2) * np.sin(dLat/2) + np.cos(np.radians(data['lat'])) * np.cos(np.radians(shifted['lat'])) * np.sin(dLon/2) * np.sin(dLon/2)
        
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)) 
    d = R * c
    d = d.sum()

    return d


def smooth(points):
    initial_value_guess = points.iloc[0]
    observation_covariance = np.diag([0.8, 0.8]) ** 2
    transition_covariance = np.diag([0.4, 0.8]) ** 2
    transition_matrix = [[1, 0], [0, 1]]
    kf = KalmanFilter(
                initial_state_mean=initial_value_guess,
                initial_state_covariance=observation_covariance,
                observation_covariance=observation_covariance,
                transition_covariance=transition_covariance,
                transition_matrices=transition_matrix)
    kalman_smoothed, _ = kf.smooth(points)
    result = pd.DataFrame(kalman_smoothed,columns=['lat','lon'])
    return result

def main():
    points = get_data(sys.argv[1])
    print('Unfiltered distance: %0.2f' % (distance(points)))
    smoothed_points = smooth(points)
    print('Filtered distance: %0.2f' % (distance(smoothed_points)))
    output_gpx(smoothed_points, 'out.gpx')

if __name__ == '__main__':
    main()