'''
Created on Mar 11, 2016

@author: pitzer
'''

import json

from luigi_tasks.base_task import RealDealBaseTask
from realdeal.redfin_client import RedfinClient

class UpdateRedfinData(RealDealBaseTask):
  
  def output(self):
    return self.getLocalFileTarget("properties_with_redfin_data.json")
  
  def run(self):
    redfin = RedfinClient()
    with self.input().open() as fin, self.output().open('w') as fout:
      properties_in = json.load(fin)
      properties_out = []
      for prop, _ in redfin.updatePropertiesWithRedfinData(properties_in):
        properties_out.append(prop)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)