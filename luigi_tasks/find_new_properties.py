'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import os

from time import localtime, strftime

from luigi_tasks.base_task import RealDealBaseTask
from fusion_tables_client import FusionTablesClient


class FindNewProperties(RealDealBaseTask):
  __ROW_KEY = "realtor_property_id"
  __client = None
  __cached_row_keys = set()
  
  def output(self):
    return self.getLocalFileTarget("properties_new.json")
          
  def initializeFusionTable(self):
    self.__client = FusionTablesClient(os.environ["REALDEAL_SERVICE_ACCOUNT"],
                                       os.environ["REALDEAL_PRIVATE_KEY"], 
                                       os.environ["REALDEAL_FUSION_TABLE_ID"])
    rows = self.__client.getRows(columns=[self.__ROW_KEY])
    for row in rows:
      self.__cached_row_keys.add(row[self.__ROW_KEY])
  
  def propertyInFusionTable(self, prop):
    return prop[self.__ROW_KEY] in self.__cached_row_keys
                    
  def run(self):
    with self.input().open() as fin, self.output().open('w') as fout:
      properties_in = json.load(fin)
      self.initializeFusionTable()
      
      properties_out = []
      for prop in properties_in:
        if not self.propertyInFusionTable(prop):
          prop["created"] = strftime("%Y-%m-%d %H:%M:%S", localtime())
          prop["last_update"] = prop["created"]
          properties_out.append(prop)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)

