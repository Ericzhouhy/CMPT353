import os
import pathlib
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from xml.etree import ElementTree as ET

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'
    
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.10f' % (pt['lon']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))

def get_data(input_gpx):
    parse_result = ET.parse(input_gpx)
    position = []
    times = []

    for item in parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt'):
        position.append({'lat':float(item.attrib['lat']),'lon':float(item.attrib['lon'])})

    pos_df = pd.DataFrame(position,columns = ['lat','lon'])

    for item in parse_result.iter('{http://www.topografix.com/GPX/1/0}time'):
        times.append({'timestamp': item.text})
    
    time_df = pd.DataFrame(times, columns = ['timestamp'])
    
    points = pd.merge(pos_df, time_df, how = 'inner', left_index=True, right_index=True)
    
    points['timestamp'] = pd.to_datetime(points['timestamp'], utc=True)
    return points


def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])
    
    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    first_time = accl['timestamp'].min()

    offsetList = np.linspace(-5.0,5.0,101)
    vals = []
    
    for offset in offsetList:

        new_accl = accl
        new_gps = gps
        new_phone = phone

        new_phone['timestamp'] = first_time + pd.to_timedelta(new_phone['time']+offset, unit='sec') 

        new_phone['timestamp'] = new_phone['timestamp'].dt.round("4S")
        new_gps['timestamp'] = new_gps['timestamp'].dt.round("4S")
        new_accl['timestamp'] = new_accl['timestamp'].dt.round("4S")

        new_phone = new_phone.groupby(['timestamp']).mean()
        new_gps = new_gps.groupby(['timestamp']).mean()
        new_accl = new_accl.groupby(['timestamp']).mean()

        combined = pd.merge(left = new_phone, right = new_gps, left_on = new_phone.index, right_on = new_gps.index)
        combined = combined.rename(columns = {'key_0' : 'datetime'}).set_index(['datetime'])
        combined = pd.merge(left = combined, right = new_accl, left_on = combined.index, right_on = new_accl.index)
        combined = combined.rename(columns = {'key_0' : 'datetime'}).set_index(['datetime'])

        res = combined['gFx']*combined['x']
        vals.append(res.sum())
    
    best_offset = offsetList[np.where(vals == np.max(vals))[0][0]]
    
    combined = combined.reset_index()

    print(f'Best time offset: {best_offset:.1f}')
    os.makedirs(output_directory, exist_ok=True)
    output_gpx(combined[['datetime', 'lat', 'lon']], output_directory / 'walk.gpx')
    combined[['datetime', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)


main()