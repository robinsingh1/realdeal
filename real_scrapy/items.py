# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class RealScrapyItem(scrapy.Item):
  location = scrapy.Field()
  address = scrapy.Field()
  city = scrapy.Field()
  state = scrapy.Field()
  zip = scrapy.Field()
  purchase_price = scrapy.Field()
  bedrooms = scrapy.Field()
  bathrooms = scrapy.Field()
  building_size = scrapy.Field()
  lot_size = scrapy.Field()
  image = scrapy.Field()
  realtor_url = scrapy.Field()
  realtor_property_id = scrapy.Field()


class CraigslistItem(scrapy.Item):
    title = scrapy.Field()
    link = scrapy.Field()
    price = scrapy.Field()
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    building_size = scrapy.Field()
    craigslist_id = scrapy.Field()
    posting_date = scrapy.Field()    
    latitude = scrapy.Field()
    longitude = scrapy.Field()
