'''
Created on Mar 6, 2016

@author: pitzer
'''
import copy
import logging
import os.path


from pyzillow.pyzillow import ZillowWrapper, GetDeepSearchResults
from pyzillow.pyzillowerrors import ZillowError
from retrying import retry
from time import localtime, strftime
from xml.etree import cElementTree as ElementTree  # for zillow API

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

ZILLOW_NOT_FOUND_RESPONSE = """<?xml version="1.0" encoding="utf-8"?><SearchResults:searchresults xsi:schemaLocation="http://www.zillow.com/static/xsd/SearchResults.xsd http://www.zillowstatic.com/vstatic/dc6fc1b/static/xsd/SearchResults.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:SearchResults="http://www.zillow.com/static/xsd/SearchResults.xsd"><message><text>All good</text><code>0</code></message><response><results><result><zpid>-1</zpid></result></results></response></SearchResults:searchresults>"""


def isRetryableException(exception):
  """Return True if exception is a retryable internal error."""
  return (isinstance(exception, ZillowError) and 
          exception.status in [3, 4, 505])


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

  
  @retry(retry_on_exception=isRetryableException, 
         wait_exponential_multiplier=1000, 
         wait_exponential_max=10000)
  def getDeepSearchResults(self, address, citystatezip, rentzestimate=True):
    self.rate_limiter.limit()
    deep_search_response = self.zillow_wrapper.get_deep_search_results(
        address=address, 
        zipcode=citystatezip, 
        rentzestimate=rentzestimate)
    return GetDeepSearchResults(deep_search_response)
        
  def updatePropertiesWithZillowData(self, properties, yield_all=False):  
    for prop in properties:
      updated_prop = copy.deepcopy(prop)
      is_updated = False
      result = None
      logging.info("Updating: %s, %s, %s", 
                    prop["address"], prop["city"], prop["zip"])
      
      if prop["address"] and prop["city"] and prop["zip"]:
        try:
          result = self.getDeepSearchResults(
              address=prop["address"], 
              citystatezip=prop["city"] + ", " + prop["zip"])
        except ZillowError as e: 
          logging.error("No Zillow data found for: %s, %s, %s", 
                        prop["address"], prop["city"], prop["zip"])
          logging.error("ZillowError: %s", e.message)
          # Set invalid zillow-id if no exact match was found for input address
          if e.status in [500, 501, 502, 503, 504, 506, 507, 508]:
            result = GetDeepSearchResults(
                data=ElementTree.fromstring(ZILLOW_NOT_FOUND_RESPONSE))

      if result:
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
      
      
      if is_updated or yield_all:
        yield updated_prop