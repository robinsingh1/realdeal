'''
Created on May 7, 2016

@author: pitzer
'''
import csv
import json
import luigi
import time

from realdeal.luigi.base_task import RealDealBaseTask
from realdeal.redfin_client import RedfinClient

FUSION_FIELDS = [
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

class ScrapeRedfin(RealDealBaseTask):
  regions = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties_scraped.json")
                
  def run(self):
    output = self.output()
    output.makedirs()
    redfin = RedfinClient()
    properties_all = []
    with self.output().open('w') as fout:
      for region in self.regions:
        properties = redfin.getSalesRecords(region_id=region)
        properties_all += properties
        region_filename = 'sales_records_%d.csv' % region
        region_filename_target = self.getLocalFileTarget(region_filename)
        with region_filename_target.open('w') as fout_region:
          dict_writer = csv.DictWriter(fout_region, FUSION_FIELDS)
          dict_writer.writeheader()
          dict_writer.writerows(properties)
          print "region: %d, # properties: %d" % (region, len(properties))
        time.sleep(5)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_all])
      fout.write(json_str)
    