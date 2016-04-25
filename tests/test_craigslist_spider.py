'''
Created on Apr 24, 2016

@author: pitzer
'''
import unittest

from response_utils import fake_response_from_file
from real_scrapy.spiders.craigslist_spider import CraigslistSpider

  
class CraigslistSpiderTest(unittest.TestCase):

  def setUp(self):
    self.spider = CraigslistSpider()

  def test_parse(self):
    response = fake_response_from_file(
        'responses/craigslist_rentals_hayward.html')
    results = self.spider.parse(response)
    
    count = 0
    for request in results:
      item = request.meta['item']
      self.assertIsNotNone(item['craigslist_id'])
      self.assertIsNotNone(item['title'])
      self.assertIsNotNone(item['price'])
      self.assertIsNotNone(item['link'])
      count += 1
    self.assertEqual(count, 99)
    
  def test_parse_item_page(self):
    response = fake_response_from_file(
        'responses/craigslist_rentals_hayward_5530420563.html')
    response.meta['item'] = {}
    
    item = self.spider.parse_item_page(response)
    self.assertEqual(item['latitude'], 37.649932)
    self.assertEqual(item['longitude'], -122.028772)
    self.assertEqual(item['bedrooms'], 4)
    self.assertEqual(item['bathrooms'], 3.0)
    self.assertEqual(item['building_size'], 2650)
    self.assertEqual(item['posting_date'], u'2016-04-08T20:17:38-0700')
    
    