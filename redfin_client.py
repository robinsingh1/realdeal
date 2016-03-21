'''
Created on Mar 12, 2016

@author: pitzer
'''
import copy
import datetime
import json
import logging
import requests
import urllib

from rate_limiter import RequestRateLimiter
from time import localtime, strftime

REDFIN_FIELDS = [
  "redfin_property_id",
  "redfin_listing_id",
  "redfin_url",
  "status",
  "days_on_redfin",
  "cumulative_days_on_market",
  "redfin_estimate_amount",
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
  'Cookie' :'RF_BROWSER_ID=QaVtiTOg6YixEQDUDSxtvbb%2BMT6HbIl%2B%2FzSiRH799FJexklT%2FuzRBw%3D%3D; RF_LAST_USER_ACTION=1454385772141%3A2efc6be878d6e75a5f2a47eab2ba1eecf363b55c; RF_PARTY_ID=7163720; RF_AUTH=631aeeedcc8b891f6433faad4426a49cfb9a05f9; RF_SECURE_AUTH=325087d5d9a77b7b8d4e55cd6c57fe74869b300e; RF_ALERTS_AWARENESS_SESSION=1454385792863; RF_HIDE_LDP_REG_DIV=true; lastSearch=uipt%3D1%26sf%3D1%252C2%252C5%252C6%252C3%252C4%26max_price%3D400000%26lat%3D37.699376685981264%26long%3D-122.16789884492755%26zoomLevel%3D11%26market%3Dsanfrancisco; bounding_box=-122.37252%2C37.552%2C-121.96328%2C37.84104; RF_LAST_ACCESS=1454388965926%3Acd5fd7e0390161245b4ceee7bd92f95ec7454bdb; fbm_161006757273279=base_domain=.redfin.com; __gads=ID=e59568e8f61038e8:T=1454865954:S=ALNI_Mb2Hpb02r6YKAqhqWr1n1saqspnoQ; shared_search_intros="dec=1453356793597&1414098203=1453356793597&ipc=1"; displayMode=0; sortOrder=1; sortOption=special_blend; RF_LISTING_VIEWS=56480522; userPreferences=parcels%3Dtrue%26schools%3Dfalse%26mapStyle%3Ds%26statistics%3Dtrue%26agcTooltip%3Dfalse%26agentReset%3Dfalse%26ldpRegister%3Dfalse%26afCard%3D2%26schoolType%3D0%26lastSeenLdp%3DwithSharedSearchCookie; __utmx=222895640.zZS2PIwhTXid5otj8pOSDg$0:0.4C40qc0mSzyFZQxmgsdcBw$0:1.UVb6cyQYS6-Ujwa1UeUsBQ$0:2.K-ecdN3ZRoSWnj1z4w2oHw$0:1.vD0imneuSZ-cHqIU6S634Q$0:1.1Usbn3w6QVG0ZGgsOczPCg$0:2.YfDQjlgrQ8WtCvQqVc5Xrg$0:0.W00Ol0ZXQEOv4vtQ6Hdcgw$0:1.C5t0Fp-JTGOEqbjxVH0erQ$0:1.-RuE_OgGQmWxbPKBZRIv8w$0:2; __utmxx=222895640.zZS2PIwhTXid5otj8pOSDg$0:1455561546:8035200:.4C40qc0mSzyFZQxmgsdcBw$0:1455561546:8035200:.UVb6cyQYS6-Ujwa1UeUsBQ$0:1454904494:8035200:.K-ecdN3ZRoSWnj1z4w2oHw$0:1456264915:8035200:.vD0imneuSZ-cHqIU6S634Q$0:1455118068:8035200:.1Usbn3w6QVG0ZGgsOczPCg$0:1455245743:8035200:.YfDQjlgrQ8WtCvQqVc5Xrg$0:1456264915:8035200:.W00Ol0ZXQEOv4vtQ6Hdcgw$0:1458304869:8035200.C5t0Fp-JTGOEqbjxVH0erQ$0:1458094348:8035200:.-RuE_OgGQmWxbPKBZRIv8w$0:1458304867:8035200:; unifiedLastSearch=name%3DPiedmont%2520Pines%26url%3D%252Fneighborhood%252F2142%252FCA%252FOakland%252FPiedmont-Pines%26id%3D9_2142%26type%3D6%26isSavedSearch%3D; RF_BUSINESS_MARKET=2; RF_ACCESS_LEVEL=3; _ga=GA1.2.117738853.1454385774; JSESSIONID=1C20B5EC8F390DD96536209518348175; testCookies=enabled; fbsr_161006757273279=nzywSHfAFfvteiKYv42k0QHAO5MQRpslAOulQe5uagQ.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImNvZGUiOiJBUUM3OEtfMTRoTDQ0SGw4OXJtWHJILUJFc0YxVVY4RHRwM1hUSW5JTWtnVEFjQnVXeW1OZk9LY3VUVnRyUHA0SV92eXZQUWE1SEQ5akY2OTFtazJkVXVxcFhMWUQwNjNrNU93X0NiQm5ET1A5WjJpUmhJWl90YW9aVm1XY3lFRFN4QXFxSjBHR3JDdUZuNEk3VkVtd2RiSXVvaW9vVUpsUjlXUWlmbDNyN0dCQlZmLWRUSl9OTmg3TXAwd296SlZxaWhmLVk5Zkh0eXpqNDB5ZkFPbXVyOXVuMzctTkhFRENjMHFFdDZfclFlTDJPMDdHSzBONnFacVV2dmlzZFRVaGdZdEk4WmNydG9CZzhVR1dhSUM5eEp4RkJ5LU1oMHFlN1RUOXRpWWFlcXJrdTA3eG5rWjlVTUFBTFZJcEZXQ2EzUEg0bVEzNEpQTFVhT2RQc2ZZenNqUCIsImlzc3VlZF9hdCI6MTQ1ODUxMDA2OSwidXNlcl9pZCI6IjEwMTUzNzQ5NDU1ODM0MTUzIn0; RF_MARKET=sanfrancisco; RF_BROWSER_CAPABILITIES=%7B%22css-transitions%22%3Atrue%2C%22css-columns%22%3Atrue%2C%22css-generated-content%22%3Atrue%2C%22css-opacity%22%3Atrue%2C%22events-touch%22%3Afalse%2C%22geolocation%22%3Atrue%2C%22screen-size%22%3A4%2C%22screen-size-tiny%22%3Afalse%2C%22screen-size-small%22%3Afalse%2C%22screen-size-medium%22%3Afalse%2C%22screen-size-large%22%3Afalse%2C%22screen-size-huge%22%3Atrue%2C%22html-prefetch-in-head%22%3Afalse%2C%22html-prefetch-in-iframe%22%3Atrue%2C%22html-range%22%3Atrue%2C%22html-form-validation%22%3Atrue%2C%22html-form-validation-with-required-notice%22%3Atrue%2C%22html-input-placeholder%22%3Atrue%2C%22html-input-placeholder-on-focus%22%3Atrue%2C%22ios-app-store%22%3Afalse%2C%22google-play-store%22%3Afalse%2C%22ios-web-view%22%3Afalse%2C%22android-web-view%22%3Afalse%2C%22activex-object%22%3Atrue%2C%22webgl%22%3Atrue%2C%22ios-app%22%3Afalse%2C%22history%22%3Atrue%7D'
}


_REDFIN_QUOTA_MAX_REQUESTS = 10
_REDFIN_QUOTA_TIME_INTERVAL = 60


class RedfinClient(object):
  '''
  classdocs
  '''
  def __init__(self):
    self.rate_limiter = RequestRateLimiter(max_requests=_REDFIN_QUOTA_MAX_REQUESTS, 
                                           time_interval=_REDFIN_QUOTA_TIME_INTERVAL)
  
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
      
      if redfin_info:
        is_updated = False  
        for field in REDFIN_FIELDS:
          old_value = prop.get(field, "")
          new_value = redfin_info.get(field, "")
          
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
      
      yield updated_prop
        
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
      prop["address"] = result["streetAddress"]["assembledAddress"]
    if "city" in result:
      prop["city"] = result["city"]
    if "state" in result:
      prop["state"] = result["state"]    
    if "zip" in result:
      prop["zip"] = result["zip"]
  
    # General property info      
    if "beds" in result:
      prop["beds"] = result["beds"]
    if "baths" in result:
      prop["baths"] = result["baths"]    
    if "yearBuilt" in result:
      prop["year_built"] = result["yearBuilt"] 
    if "yearBuilt" in result:
      prop["year_built"] = result["yearBuilt"] 
    if "lotSize" in result:
      prop["lot_size"] = result["lotSize"] 
    if "soldDate" in result:
      s = int(result["soldDate"]) / 1000.0
      prop["last_sold_date"] = datetime.datetime.fromtimestamp(s).strftime('%m/%d/%Y')
      
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
  
  