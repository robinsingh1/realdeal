'''
Created on Feb 10, 2016


@author: pitzer
'''

import os
import scrapy
import urllib

from realdeal.scrapy.items import RealScrapyItem


class RealtorSpider(scrapy.Spider):
  name = "realtor"
  allowed_domains = ["realtor.com"]
  
  locations = [
    "Menlo-Park_CA",
    "Redwood-City_CA",
    "San-Jose_CA",
    "East-Palo-Alto_CA",
    "Hayward_CA",
    "Fremont_CA",
    "Union-City_CA",
    "Milpitas_CA",
    "Berkeley_CA",
    "Oakland_CA",
  ]  
  base_url = os.getenv("REALTOR_SPIDER_BASE_URL", "")
  start_urls = []
  for l in locations:
    # scrape first page
    location_url = base_url.format(location=urllib.quote_plus(l))
    start_urls.append(location_url)
  
  def parse(self, response):

    item_extractions = {
      "address": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-street-address \")]/text()",
      "city": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-city \")]/text()",
      "state": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-region \")]/text()",
      "zip": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-postal \")]/text()",
      "purchase_price": ".//li[@data-label='property-price']/text()",
      "bedrooms": ".//li[@data-label='property-meta-beds']/span[@class='data-value']/text()",
      "bathrooms": ".//li[@data-label='property-meta-baths']/span[@class='data-value']/text()",
      "building_size": ".//li[@data-label='property-meta-sqft']/span[@class='data-value']/text()",
      "lot_size": ".//li[@data-label='property-meta-lotsize']/span[@class='data-value']/text()",
      "image": ".//img[@style][notw(@id)][contains(concat(' ',normalize-space(@class),' '),\" js-srp-listing-photos \")]/text()",
    }
    required_fields = ["address", "city", "state", "zip", "purchase_price"]
  
    for itemscope in response.xpath('//div[@itemscope]'):
      itemtype = itemscope.xpath('@itemtype')[0].extract()
      if itemtype != "http://schema.org/SingleFamilyResidence": 
        continue

      item = RealScrapyItem()
      item["realtor_url"] = "http://www.realtor.com" + itemscope.xpath('@data-url').extract()[0]
      item["realtor_property_id"] = itemscope.xpath('@data-propertyid').extract()[0]

      
      for extraction, extraction_xpath in item_extractions.iteritems():
        extraction_selector = itemscope.xpath(extraction_xpath)
        if extraction_selector:
          extracted_value = extraction_selector[0].extract().strip()
          if not extracted_value:
            continue
          if extraction in ["bedrooms", "building_size"]:
            extracted_value = int(extracted_value.replace(",", ""))
          if extraction in ["lot_size"]:
            extracted_value = float(extracted_value.replace(",", ""))
            if extracted_value < 100.0:
              extracted_value = int(43560.0 * extracted_value)
            else:
              extracted_value = int(extracted_value)
          if extraction == "bathrooms":
            extracted_value = float(extracted_value.replace("+", ".5"))
          if extraction == "purchase_price":
            extracted_value = int(extracted_value.strip("$").replace(",", ""))
          
          item[extraction] = extracted_value
               
      if all(x in item for x in required_fields):
        item["location"] = ", ".join([item["address"], item["city"], item["state"], item["zip"]])
        yield item
    