'''
Created on Apr 24, 2016

@author: pitzer
'''
import unittest

from response_utils import fake_response_from_file
from realdeal.scrapy.spiders.realtor_spider import RealtorSpider

  
class CraigslistSpiderTest(unittest.TestCase):

  def setUp(self):
    self.spider = RealtorSpider()

  def test_parse(self):
    response = fake_response_from_file(
        'responses/realtor_hayward.html')
    results = self.spider.parse(response)
    
    count = 0
    for item in results:
      self.assertIsNotNone(item['address'])
      self.assertIsNotNone(item['city'])
      self.assertIsNotNone(item['state'])
      self.assertIsNotNone(item['zip'])
      self.assertIsNotNone(item['purchase_price'])
      self.assertIsNotNone(item['bedrooms'])
      self.assertIsNotNone(item['bathrooms'])
      self.assertIsNotNone(item['building_size'])
      count += 1
    self.assertEqual(count, 47)
    