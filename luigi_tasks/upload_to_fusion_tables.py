'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import luigi

from fusion_tables_client import FusionTablesClient
from luigi_tasks.base_task import RealDealBaseTask


class UploadToFusionTables(RealDealBaseTask):
  p12_file = luigi.Parameter()
  service_account = luigi.Parameter()
  table_id = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties_uploaded_to_fusion.json")
                    
  def run(self):
    client = FusionTablesClient(
        self.p12_file, self.service_account, self.table_id)
    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      client.insertRows(properties)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties])
      fout.write(json_str)
      print "%d properties new." % len(properties)