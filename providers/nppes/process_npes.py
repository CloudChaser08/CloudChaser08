#!/usr/bin/env python2.7
import csv

writer = csv.writer(open('npes_providers.csv', 'w'))
filename = 'npidata_20050523-20150712.csv'
with open(filename, 'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    try:
      if row['Entity Type Code'] == "1":
        writer.writerow([row['Provider First Name'],
          row['Provider Last Name (Legal Name)'],
          row['Provider First Line Business Practice Location Address'],
          row['Provider Business Practice Location Address City Name'],
          row['Provider Business Practice Location Address State Name'],
          row['Provider Business Practice Location Address Postal Code'][:5],
          row['Provider Business Practice Location Address Telephone Number'],
          row['Provider Gender Code'],
          row['Provider Enumeration Date']])
    except:
      pass
