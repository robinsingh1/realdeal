'''
Created on Mar 20, 2016

@author: pitzer
'''
import mock
import os
import redfin_client
import requests
import unittest

class TestRedfinClient(unittest.TestCase):
  
  @mock.patch.object(requests, 'get')
  def testGetSalesRecords(self, mock_requests):
    
    with open(os.path.join(os.path.dirname(__file__), 'gis_response.txt')) as f:
      response = mock.MagicMock() 
      response.content = f.read()
      mock_requests.return_value = response
    
    client = redfin_client.RedfinClient()
    records = client.getSalesRecords(region_id='foo', region_type='bar')
    self.assertEquals(2, len(records))
