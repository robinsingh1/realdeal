'''
Created on Mar 20, 2016

@author: pitzer
'''
import mock
import os
import requests
import unittest

from realdeal.redfin_client import RedfinClient

class TestRedfinClient(unittest.TestCase):
  
  @mock.patch.object(requests, 'get')
  def testGetSalesRecords(self, mock_requests):
    responses_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(responses_dir, 'responses/redfin_gis_response.txt')
    with open(file_path) as f:
      response = mock.MagicMock() 
      response.content = f.read()
      mock_requests.return_value = response
    
    client = RedfinClient()
    records = client.getSalesRecords(region_id='foo', region_type='bar')
    self.assertEquals(2, len(records))
