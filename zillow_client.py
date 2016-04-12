'''
Created on Mar 6, 2016

@author: pitzer
'''
import copy
import logging
import os.path


from pyzillow.pyzillow import ZillowWrapper, GetDeepSearchResults
from pyzillow.pyzillowerrors import ZillowError
from time import localtime, strftime

from rate_limiter import RequestRateLimiter


ZILLOW_RATE_LIMITER_FILE = "zillow_rate_limits.json"
ZILLOW_QUOTA_MAX_REQUESTS = 1000
ZILLOW_QUOTA_TIME_INTERVAL = 60 * 60 * 24

ZILLOW_FIELDS = [
  "rowid",
  "address",
  "city",
  "state",
  "zip",
  "zillow_id",
  "zillow_url",
  "home_type",
  "latitude",
  "longitude",
  "tax_year",
  "tax_value",
  "year_built",
  "last_sold_date",
  "last_sold_price",
  "zestimate_amount",
  "zestimate_last_updated",
  "zestimate_value_change",
  "zestimate_valuation_range_high",
  "zestimate_valuation_range_low",
  "zestimate_percentile",
  "rentzestimate_amount",
  "rentzestimate_last_updated",
  "rentzestimate_value_change",
  "rentzestimate_valuation_range_high",
  "rentzestimate_valuation_range_low",
  "region_name",
  "region_type",
]


class ZillowClient(object):
  '''
  classdocs
  '''
  
  def __init__(self, zillow_api_key):
    if os.path.isfile(ZILLOW_RATE_LIMITER_FILE):
      self.rate_limiter = RequestRateLimiter.fromFile(ZILLOW_RATE_LIMITER_FILE)
    else:
      self.rate_limiter = RequestRateLimiter(max_requests=ZILLOW_QUOTA_MAX_REQUESTS, 
                                             time_interval=ZILLOW_QUOTA_TIME_INTERVAL,
                                             state_file=ZILLOW_RATE_LIMITER_FILE)
    self.zillow_wrapper = ZillowWrapper(zillow_api_key)


  def updatePropertiesWithZillowData(self, properties, yield_all=False):  
    for prop in properties:
      logging.info("address: %s", prop["address"])
      self.rate_limiter.limit()
      deep_search_response = None
      try:
        deep_search_response = self.zillow_wrapper.get_deep_search_results(
            address=prop["address"], 
            zipcode=prop["city"] + ", " + prop["zip"], 
            rentzestimate=True)
      except ZillowError as e: 
        logging.error("ZillowError: %s", e.message)
      
      updated_prop = copy.deepcopy(prop)
      is_updated = False
      if deep_search_response:
        result = GetDeepSearchResults(deep_search_response)
        for field in ZILLOW_FIELDS:
          # Map property fields to Zillow fields.
          if field == "zillow_url":
            zillow_field = "home_detail_link"
          else:
            zillow_field = field
          
          if not hasattr(result, zillow_field):
            continue
          
          old_value = prop.get(field, "")
          new_value = getattr(result, zillow_field)
          
          # Only update fields that have changed.
          if (new_value != None and str(new_value) != str(old_value)):
            logging.info("%s: %s -> %s", field, old_value, new_value)
            updated_prop[field] = new_value
            is_updated = True 
                
        if is_updated:
          logging.info("Updating: %s", prop["address"])
          updated_prop["last_update"] = strftime("%Y-%m-%d %H:%M:%S", localtime())
          updated_prop["zillow_last_update"] = updated_prop["last_update"]
      
      else:
        logging.error("No Zillow property info found for: %s.", prop["address"])
      
      if is_updated or yield_all:
        yield updated_prop