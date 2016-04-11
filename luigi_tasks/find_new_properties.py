'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import luigi

from time import localtime, strftime

from luigi_tasks.base_task import RealDealBaseTask
from fusion_tables_client import FusionTablesClient


class FindNewProperties(RealDealBaseTask):
  __cached_row_keys = set()
  
  fusion_service_account = luigi.Parameter()
  fusion_private_key = luigi.Parameter()
  fusion_table_id = luigi.Parameter()
  key_columns = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties_new.json")
          
  def initializeFusionTable(self):
    client = FusionTablesClient(self.fusion_service_account,
                                self.fusion_private_key, 
                                self.fusion_table_id)
    rows = client.getRows(columns=self.key_columns.split(","))
    for row in rows:
      self.__cached_row_keys.add(self.rowKey(row))
  
  def rowKey(self, row):
    return ":".join([unicode(row.get(c)) for c in self.key_columns.split(",")])
  
  def propertyInFusionTable(self, prop):
    key = self.rowKey(prop)
    return key in self.__cached_row_keys
                    
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
          self.__cached_row_keys.add(self.rowKey(prop))
          
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)

