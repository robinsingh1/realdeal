'''
Created on Apr 24, 2016

@author: pitzer
'''
import unittest

from response_utils import fake_response_from_file
from realdeal.scrapy.spiders.craigslist_spider import CraigslistSpider

  
class CraigslistSpiderTest(unittest.TestCase):

  def setUp(self):
    self.spider = CraigslistSpider()
  
  def test_parse(self):
    response = fake_response_from_file('responses/craigslist_rentals.html')
    results = list(self.spider.parse(response))
    
    count = 0
    for request in results:
      item = request.meta['item']
      self.assertIsNotNone(item['craigslist_id'])
      self.assertIsNotNone(item['title'])
      self.assertIsNotNone(item['price'])
      self.assertIsNotNone(item['link'])
      count += 1
    self.assertEqual(count, 99)
    
    self.assertEqual('Huge top floor 1 bedroom apartment near downtown Redwood City!',
                     results[0].meta['item']['title'])
    self.assertEqual(3703.0, results[0].meta['item']['price'])
    self.assertEqual('http://sfbay.craigslist.org/pen/apa/5883043515.html',
                     results[0].meta['item']['link'])
    self.assertEqual(5883043515, results[0].meta['item']['craigslist_id'])
      
  def test_parse_item_page(self):
    response = fake_response_from_file(
        'responses/craigslist_rentals_item_page.html')
    response.meta['item'] = {}
    
    item = self.spider.parse_item_page(response)
    self.assertEqual(37.491212, item['latitude'])
    self.assertEqual(-122.230883, item['longitude'])
    self.assertEqual(1, item['bedrooms'])
    self.assertEqual(1.0, item['bathrooms'])
    self.assertEqual(937, item['building_size'])
    self.assertEqual(u'2016-11-18T17:54:07-0800', item['posting_date'])
    
    