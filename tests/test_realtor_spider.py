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
    response = fake_response_from_file('responses/realtor_single_family.html')
    results = list(self.spider.parse(response))
    
    item = results[0]
    self.assertIsNotNone(item['address'])
    self.assertIsNotNone(item['city'])
    self.assertIsNotNone(item['state'])
    self.assertIsNotNone(item['zip'])
    self.assertIsNotNone(item['purchase_price'])
    self.assertIsNotNone(item['bedrooms'])
    self.assertIsNotNone(item['bathrooms'])
    self.assertIsNotNone(item['building_size'])
    self.assertEqual(len(results), 47)

  def test_no_bathroom(self):
    response = fake_response_from_file('responses/realtor_multi_family.html')
    results = list(self.spider.parse(response))
    
    item = results[0]
    self.assertIsNotNone(item['address'])
    self.assertIsNotNone(item['city'])
    self.assertIsNotNone(item['state'])
    self.assertIsNotNone(item['zip'])
    self.assertIsNotNone(item['purchase_price'])
    self.assertIsNotNone(item['bedrooms'])
    self.assertNotIn('bathrooms', item)
    self.assertIsNotNone(item['building_size'])
    self.assertEqual(len(results), 1)
        