#!env python

import csv
import sys

zc = {}


with open('zips.csv', 'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    zip3 = row['zip'][:3]
    #print zip3, ' : ', row['latitude_min'], '-', row['latitude_max'], ' : ', row['longitude_min'], '-', row['longitude_max']
    lat_min = float(row['latitude_min']) if row['latitude_min'] else None
    lat_max = float(row['latitude_max']) if row['latitude_max'] else None
    lon_min = float(row['longitude_min']) if row['longitude_min'] else None
    lon_max = float(row['longitude_max']) if row['longitude_max'] else None
    if zip3 not in  zc.keys():
      zc[zip3] = {
        'lat_min': lat_min,
        'lat_max': lat_max,
        'lon_min': lon_min,
        'lon_max': lon_max
      }
    else:
      zc[zip3] = {
        'lat_min': lat_min if lat_min < zc[zip3]['lat_min'] or zc[zip3]['lat_min'] == None else zc[zip3]['lat_min'],
        'lat_max': lat_max if lat_max > zc[zip3]['lat_max'] or zc[zip3]['lat_max'] == None else zc[zip3]['lat_max'],
        'lon_min': lon_min if lon_min < zc[zip3]['lon_min'] or zc[zip3]['lon_min'] == None else zc[zip3]['lon_min'],
        'lon_max': lon_max if lon_max > zc[zip3]['lon_max'] or zc[zip3]['lon_max'] == None else zc[zip3]['lon_max']
      }


csvwriter = csv.writer(sys.stdout)
csvwriter.writerow(['zip3', 'lat_center', 'lon_center'])
for k in zc.keys():
  r = zc[k]
  lat = r['lat_min'] + ((r['lat_max'] - r['lat_min'])/2) if r['lat_min'] and r['lat_max'] else None;
  lon = r['lon_min'] + ((r['lon_max'] - r['lon_min'])/2) if r['lon_min'] and r['lon_max'] else None;
#  csvwriter.writerow([k, r['lat_min'], r['lon_min'], r['lat_max'], r['lon_max']])
  csvwriter.writerow([k, lat, lon])
  
