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
    response = fake_response_from_file(
        'responses/craigslist_rentals_hayward.html')
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
    
    self.assertEqual('**Spacious Hayward Home **Bonus Room**OPEN SAT**',
                     results[0].meta['item']['title'])
    self.assertEqual(3000.0, results[0].meta['item']['price'])
    self.assertEqual('http://sfbay.craigslist.org/eby/apa/5772672614.html',
                     results[0].meta['item']['link'])
    self.assertEqual(5772672614, results[0].meta['item']['craigslist_id'])
    
  def test_parse_item_page(self):
    response = fake_response_from_file(
        'responses/craigslist_rentals_5772672614.html')
    response.meta['item'] = {}
    
    item = self.spider.parse_item_page(response)
    self.assertEqual(item['latitude'], 37.647623)
    self.assertEqual(item['longitude'], -122.113736)
    self.assertEqual(item['bedrooms'], 3)
    self.assertEqual(item['bathrooms'], 3.0)
    self.assertEqual(item['building_size'], 2264)
    self.assertEqual(item['posting_date'], u'2016-09-08T14:15:52-0700')
    
    