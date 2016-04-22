'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import os

from luigi_tasks.base_task import RealDealBaseTask
from realdeal.zillow_client import ZillowClient

class UpdateZillowData(RealDealBaseTask):
  
  def output(self):
    return self.getLocalFileTarget("properties_with_zillow_data.json")
  
  def run(self):
    zillow = ZillowClient(os.environ["REALDEAL_ZILLOW_API_KEY"])
    with self.input().open() as fin, self.output().open('w') as fout:
      properties_in = json.load(fin)
      properties_out = []
      for prop, _ in zillow.updatePropertiesWithZillowData(properties_in):
        properties_out.append(prop)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)