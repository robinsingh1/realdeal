'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import luigi

from fusion_tables_client import FusionTablesClient
from luigi_tasks.base_task import RealDealBaseTask


class UploadToFusionTables(RealDealBaseTask):
  fusion_private_key = luigi.Parameter()
  fusion_service_account = luigi.Parameter()
  fusion_table_id = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties_uploaded_to_fusion.json")
                    
  def run(self):
    client = FusionTablesClient(
        self.fusion_service_account, 
        self.fusion_private_key, 
        self.fusion_table_id)
    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      client.insertRows(properties)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties])
      fout.write(json_str)
      print "%d properties new." % len(properties)