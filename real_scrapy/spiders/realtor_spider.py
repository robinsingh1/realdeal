'''
Created on Feb 10, 2016


@author: pitzer
'''

import scrapy

from real_scrapy.items import RealScrapyItem


class RealtorSpider(scrapy.Spider):
  name = "realtor"
  allowed_domains = ["realtor.com"]
  start_urls = [
    "http://www.realtor.com/realestateandhomes-search/Menlo-Park_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",            
    "http://www.realtor.com/realestateandhomes-search/Redwood-City_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/San-Jose_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/East-Palo-Alto_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Hayward_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Fremont_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Union-City_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Milpitas_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Berkeley_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
    "http://www.realtor.com/realestateandhomes-search/Oakland_CA/type-single-family-home/price-na-700000/shw-nl?pgsz=50",
#     "file:///Users/pitzer/Documents/workspace/realdeal/realtor_east_palo_alto.html",
#     "file:///Users/pitzer/Documents/workspace/realdeal/realtor_hayward.html",
#     "file:///Users/pitzer/Documents/workspace/realdeal/realtor_san_jose.html",
  ]

  def parse(self, response):
#     with open('scrape.html', 'wb') as f:
#       f.write(response.body)
    
    item_extractions = {
      "address": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-street-address \")]/text()",
      "city": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-city \")]/text()",
      "state": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-region \")]/text()",
      "zip": ".//span[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" listing-postal \")]/text()",
      "purchase_price": ".//li[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" srp-item-price srp-items-floated \")]/text()",
      "bedrooms": ".//ul[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" property-meta list-horizontal list-style-disc list-spaced \")]/li[1][not(@id)][not(@class)][not(@style)]/span[1][not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" data-value \")]/text()",
      "bathrooms": ".//ul[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" property-meta list-horizontal list-style-disc list-spaced \")]/li[2][not(@id)][not(@class)][not(@style)]/span[1][not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" data-value \")]/text()",
      "building_size": ".//ul[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" property-meta list-horizontal list-style-disc list-spaced \")]/li[3][not(@id)][not(@class)][not(@style)]/span[1][not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" data-value \")]/text()",
      "lot_size": ".//ul[not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" property-meta list-horizontal list-style-disc list-spaced \")]/li[4][not(@id)][not(@class)][not(@style)]/span[1][not(@id)][not(@style)][contains(concat(' ',normalize-space(@class),' '),\" data-value \")]/text()",
      "image": ".//img[@style][notw(@id)][contains(concat(' ',normalize-space(@class),' '),\" js-srp-listing-photos \")]/text()",
    }
    required_fields = ["address", "city", "state", "zip"]
  
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
          if extraction == "purchase_price":
            item[extraction] = float(extracted_value.strip("$").replace(",", ""))
          else:
            item[extraction] = extracted_value
            
      if all(x in item for x in required_fields):
        item["location"] = ", ".join([item["address"], item["city"], item["state"], item["zip"]])
        yield item
    