'''
Created on Mar 13, 2016

@author: pitzer
'''
import csv
import logging
import os
import time

from redfin_client import RedfinClient

FUSION_FIELS = [
      'status', 
      'sold_date', 
      'address', 
      'city',
      'state',
      'zip',
      'mls_id',
      'price',
      'building_size',
      'lot_size',
      'year_built',
      'bedrooms',
      'bathrooms',
      'redfin_property_id',
      'redfin_listing_id',
      'redfin_url',
      'latitude',
      'longitude'
    ]

REGIONS = [
  5401,     # East Palo Alto
  11961,    # Menlo Park
  24616,    # North Fairoaks
  820,      # Atherton
  8439,     # Hayward
  15525,    # Redwood City
  6671,     # Fremont
  17447,    # San Leandro
  21841,    # Castro Valley
  14986,    # Pleasenton
  5159,     # Dublin
  10683,    # Livermore
  17519,    # San Ramon
  25747,    # San Lorenzo
  21280,    # Ashland
  21906,    # Cherryland
  13111,    # Newark
  20321,    # Union City 
  12204,    # Milpitas
  1590,     # Berkeley
  13654,    # Oakland
#   117,      # Alameda
  17420,    # San Jose
  2673,     # Campbell
  17960,    # Saratoga
  4561,     # Cupertino
  11234,    # Los Gatos
  17675,    # Santa Clara
  19457,    # Sunnyvale
  12739,    # Mountain View
  11018,    # Los Altos
  11022,    # Los Altos Hills
  14325,    # Palo Alto
  15117,    # Portola Valley
  20966,    # Woodside
  16687,    # San Carlos
  1362,     # Belmont
  17490,    # San Mateo
  6524,     # Foster City
  2350,     # Burlingame
  12130,    # Milbrae
  16671,    # San Bruno
]
    
def main():
  redfin = RedfinClient()
  
  logging.info("Processing sales records.")
  properties_all = []
  for region in REGIONS:
    properties = redfin.getSalesRecords(region_id=region)
    properties_all += properties
    with open('sale_records_%d.csv' % region, 'w') as f:
      dict_writer = csv.DictWriter(f, FUSION_FIELS)
      dict_writer.writeheader()
      dict_writer.writerows(properties)
      print "region: %d, # properties: %d" % (region, len(properties))
    time.sleep(5)
    
  with open('sale_records_all.csv', 'w') as f:
    dict_writer = csv.DictWriter(f, FUSION_FIELS)
    dict_writer.writeheader()
    dict_writer.writerows(properties_all)

  print "all: %d" % len(properties_all)      

if __name__ == "__main__":
  main()
    


    
    
    
  