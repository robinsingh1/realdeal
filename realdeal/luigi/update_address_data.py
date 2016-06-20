#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Apr 10, 2016

@author: pitzer
'''

import json

from geopy.geocoders import Nominatim
from realdeal.luigi.base_task import RealDealBaseTask
from retrying import retry


class AddessGeoCoder(object):
  geolocator = Nominatim()
  
  @retry(wait_exponential_multiplier=1000, 
         wait_exponential_max=10000)
  def address(self, latitude, longitude):
    location = self.geolocator.reverse("%f, %f" % (latitude, longitude))
    if location and "address" in location.raw:
      return location.raw["address"]
          
          
class UpdateAddressData(RealDealBaseTask):
  address_geo_coder = AddessGeoCoder()
  
  def output(self):
    return self.getLocalFileTarget("properties_with_address_data.json")
  
  def run(self):
    with self.input().open() as fin, self.output().open('w') as fout:
      properties_in = json.load(fin)
      properties_out = []
      updated_properties = 0
      for prop in properties_in:
        lat = prop.get("latitude")
        lon = prop.get("longitude")
        if not lat or not lon:
          continue
    
        address = self.address_geo_coder.address(lat, lon)
        if not address:
          print "Couldn't find address for: %f, %f" % (lat, lon)
          continue
    
        zipcode = address.get("postcode")
        if not zipcode:
          print "Couldn't find zip code for: %f, %f" % (lat, lon)
          continue
    
        city = (
          address.get("city") or 
          address.get("town") or 
          address.get("village") or 
          address.get("suburb") or 
          address.get("hamlet")
        )
        if not city:    
          print "Couldn't find city for: %f, %f" % (lat, lon)
          continue

        city = unicode(city)
        if city == u"San José":
          city = u"San Jose"
        elif city == u"SF":
          city = u"San Francisco"
        prop["zipcode"] = zipcode
        prop["city"] = city   
        properties_out.append(prop)
        updated_properties += 1
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)