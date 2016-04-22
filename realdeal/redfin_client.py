'''
Created on Mar 12, 2016

@author: pitzer
'''
import copy
import datetime
import json
import logging
import os
import requests
import urllib

from realdeal.rate_limiter import RequestRateLimiter
from time import localtime, strftime

REDFIN_FIELDS = [
  "rowid",
  "address",
  "city",
  "state",
  "zip",
  "redfin_property_id",
  "redfin_listing_id",
  "redfin_url",
  "status",
  "days_on_redfin",
  "cumulative_days_on_market",
  "redfin_estimate_amount",
  "purchase_price",
  "bedrooms",
  "bathrooms",
  "year_built",
  "lot_size",
  "last_sold_date",
  "latitude",
  "longitude",
]

_LOCATION_URL = ('https://www.redfin.com/stingray/do/location-autocomplete?'
                 'location={location}&start=0&count=10&v=2&market=sanfrancisco&'
                 'al=2&iss=true')

_INITIAL_INFO_URL = ('http://www.redfin.com/stingray/api/home/details/'
                     'initialInfo?path={url}')

_CORE_PROPERTY_INFO_URL = ('https://www.redfin.com/stingray/api/home/details/'
                           'corePropertyInfo?propertyId={property_id}&'
                           'accessLevel=2')

_CORE_LISTING_INFO_URL = ('https://www.redfin.com/stingray/api/home/details/'
                          'corePropertyInfo?propertyId={property_id}&'
                          'listingId={listing_id}&accessLevel=2')

_GIS_URL = ('https://www.redfin.com/stingray/api/gis?al=2&num_homes=10000&'
            'page_number=1&region_id={region_id}&region_type={region_type}&'
            'sold_within_days={sold_within_days}&sp=true&start=0&status=1&'
            'uipt=1&v=8')


_DEFAULT_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Encoding': 'gzip, deflate, sdch',
  'Accept-Language': 'en-US,en;q=0.8,de;q=0.6',
  'Cache-Control': 'no-cache',
  'Connection': 'keep-alive',
  'Host': 'www.redfin.com',
  'Upgrade-Insecure-Requests': '1',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36',
}


_REDFIN_QUOTA_MAX_REQUESTS = 60
_REDFIN_QUOTA_TIME_INTERVAL = 60


class RedfinClient(object):
  '''
  classdocs
  '''
  def __init__(self):
    self.rate_limiter = RequestRateLimiter(max_requests=_REDFIN_QUOTA_MAX_REQUESTS, 
                                           time_interval=_REDFIN_QUOTA_TIME_INTERVAL)
    self._headers = _DEFAULT_HEADERS
    if os.environ["REALDEAL_REDFIN_COOKIE"]:
      self._headers['Cookie'] = os.environ["REALDEAL_REDFIN_COOKIE"]
      
  def getSalesRecords(self, region_id, region_type=6, sold_within_days=360):
    url = _GIS_URL.format(region_id=region_id,
                          region_type=region_type,
                          sold_within_days=sold_within_days)
    result = self.queryRedfin(url)
    
    records = []
    for home in result["homes"]:
      if "price" not in home or "value" not in home["price"]:
        print "No price found!!"
        print home
        continue
      prop = {
        "status": "",
        "sold_date": "",
        "address": "",
        "city": "",
        "state": "",
        "zip": "",
        "latitude": "",
        "longitude": "",
        "mls_id": "",
        "price": "",
        "building_size": "",
        "lot_size": "",
        "year_built": "",
        "bedrooms": "",
        "bathrooms": "",
        "redfin_property_id": "",
        "redfin_listing_id": "",
        "redfin_url": "",
      }
      if "mlsStatus" in home:
        prop["status"] = home["mlsStatus"]
      if "soldDate" in home:
        s = int(home["soldDate"]) / 1000.0
        prop["sold_date"] = datetime.datetime.fromtimestamp(s).strftime('%m/%d/%Y')
      if "streetLine" in home and "value" in home["streetLine"]:
        prop["address"] = home["streetLine"]["value"].title()
      if "city" in home:
        prop["city"] = home["city"].title()
      if "state" in home:
        prop["state"] = home["state"]    
      if "zip" in home:
        prop["zip"] = home["zip"]  
      if "latLong" in home and "value" in home["latLong"]:
        prop["latitude"] = home["latLong"]["value"]["latitude"]
        prop["longitude"] = home["latLong"]["value"]["longitude"]    

      if "mlsId" in home and "value" in home["mlsId"]:
        prop["mls_id"] = home["mlsId"]["value"]
      if "price" in home and "value" in home["price"]:
        prop["price"] = home["price"]["value"]
      if "sqFt" in home and "value" in home["sqFt"]:
        prop["building_size"] = home["sqFt"]["value"]
      if "lotSize" in home and "value" in home["lotSize"]:
        prop["lot_size"] = home["lotSize"]["value"]
      if "yearBuilt" in home and "value" in home["yearBuilt"]:
        prop["year_built"] = home["yearBuilt"]["value"]
      if "beds" in home:
        prop["bedrooms"] = home["beds"]    
      if "baths" in home:
        prop["bathrooms"] = home["baths"] 
      if "propertyId" in home:
        prop["redfin_property_id"] = home["propertyId"]
      if "listingId" in home:
        prop["redfin_listing_id"] = home["listingId"]  
      if "url" in home:
        prop["redfin_url"] = "https://www.redfin.com" + home["url"]  
      records.append(prop)

    return records
        
  def updatePropertiesWithRedfinData(self, properties):  
    for prop in properties:
      logging.info("address: %s", prop["address"])
      redfin_info = self.getCorePropertyInfo(prop["address"], 
                                             prop["city"],
                                             prop["state"])
      updated_prop = copy.deepcopy(prop)
      
      is_updated = False
      if redfin_info:  
        for field in REDFIN_FIELDS:
          old_value = prop.get(field, None)
          new_value = redfin_info.get(field, None)
          
          # Only update fields that have changed.
          if (new_value != None and str(new_value) != str(old_value)):
            logging.info("%s: %s -> %s", field, old_value, new_value)
            updated_prop[field] = new_value
            is_updated = True 
                
        if is_updated:
          logging.info("Updating: %s", prop["address"])
          updated_prop["last_update"] = strftime("%Y-%m-%d %H:%M:%S", localtime())
          updated_prop["redfin_last_update"] = updated_prop["last_update"]
      
      else:
        logging.error("No Redfin property info found for: %s.", prop["address"])
      
      yield updated_prop, is_updated
        
  def queryRedfin(self, url):
    self.rate_limiter.limit()
    response = requests.get(url=url, headers=_DEFAULT_HEADERS)
    content = response.content
    content = content.replace('{}&&', '')
    result = json.loads(content)
  
    if not "payload" in result:
      print result
      logging.error("No payload found in results for: %s.", url)
      return
    
    return result["payload"]
    
    
  def getCorePropertyInfo(self, address, city, state):
    prop = self.getRedfinProperty(address, city, state)
    if not prop:
      logging.error("Couldn't find any property info for %s", address)
      return
    
    if "redfin_listing_id" in prop:
      info_url = _CORE_LISTING_INFO_URL.format(property_id=prop["redfin_property_id"], 
                                               listing_id=prop["redfin_listing_id"])
    else:
      info_url = _CORE_PROPERTY_INFO_URL.format(property_id=prop["redfin_property_id"])
    result = self.queryRedfin(info_url)
    print result
    
    if not result:
      logging.error("Core property info request didn't return results.")
      return
    
    # Redfin details.
    prop["redfin_url"] = "https://www.redfin.com" + prop["redfin_url"] 
    
    # Listing status.
    if "status" in result:
      if "longerDefinitionToken" in result["status"]:
        prop["status"] = result["status"]["longerDefinitionToken"]
      else:
        prop["status"] = "not-for-sale"
      
  
    # purchase price
    if "priceInfo" in result and "amount" in result["priceInfo"]:
      prop["purchase_price"] = result["priceInfo"]["amount"]
    if "avmInfo" in result and "predictedValue" in result["avmInfo"]:
      prop["redfin_estimate_amount"] = result["avmInfo"]["predictedValue"]  
      
    # address  
    if "streetAddress" in result and "assembledAddress" in result["streetAddress"]:
      prop["address"] = result["streetAddress"]["assembledAddress"].title()
    if "city" in result:
      prop["city"] = result["city"].title()
    if "state" in result:
      prop["state"] = result["state"]    
    if "zip" in result:
      prop["zip"] = result["zip"]
  
    # General property info      
    if "beds" in result:
      prop["bedrooms"] = result["beds"]
    if "baths" in result:
      prop["bathrooms"] = result["baths"]    
    if "yearBuilt" in result:
      prop["year_built"] = result["yearBuilt"] 
    if "yearBuilt" in result:
      prop["year_built"] = result["yearBuilt"] 
    if "lotSize" in result:
      prop["lot_size"] = result["lotSize"] 
    if "soldDate" in result:
      s = int(result["soldDate"]) / 1000.0
      prop["last_sold_date"] = datetime.datetime.fromtimestamp(s).strftime('%m/%d/%Y')
    if "latLong" in result and "latitude" in result["latLong"]:
      prop["latitude"] = result["latLong"]["latitude"] 
    if "latLong" in result and "longitude" in result["latLong"]:
      prop["longitude"] = result["latLong"]["longitude"]    
      
    # Market info.
    if "daysOnRedfin" in result:
      prop["days_on_redfin"] = result["daysOnRedfin"] 
    if "cumulativeDaysOnMarket" in result:
      prop["cumulative_days_on_market"] = result["cumulativeDaysOnMarket"] 
    
    return prop
      
  def getRedfinProperty(self, address, city, state):
    url = self.getUrlForAddress(address, city, state)
    if not url:
      logging.error("Couldn't find a redfin url for %s", address)
      return
    result = self.queryRedfin(_INITIAL_INFO_URL.format(url=url))
    
    if "propertyId" not in result:
      logging.error("No propertyId found.")
      return
    
    prop = {}
    prop["redfin_property_id"] = result["propertyId"]
    prop["redfin_url"] = url
    
    if "listingId" in result:
      prop["redfin_listing_id"] = result["listingId"]
      
    return prop
    
  def getUrlForAddress(self, address, city, state):
    url = _LOCATION_URL.format(location=urllib.quote(address))
    result = self.queryRedfin(url)
    print result
    
    if "sections" in result and len(result["sections"]) > 0:
      first_section = result["sections"][0]
      if "rows" in first_section and len(first_section["rows"]) > 0:
        location = first_section["rows"][0]
        if "url" in location:
          return location["url"]
      

if __name__ == "__main__":
  client = RedfinClient()
  info = client.getCorePropertyInfo('2098 Camperdown Way', 'Union City', 'CA')
  print info
  
  