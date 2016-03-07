'''
Created on Feb 21, 2016

@author: pitzer
'''

import json
import os

from fusion_tables_client import FusionTablesClient
from luigi_tasks.base_task import RealDealBaseTask


class UploadToFusionTables(RealDealBaseTask):
  
  def output(self):
    return self.getLocalFileTarget("properties_uploaded_to_fusion.json")
                    
  def run(self):
    client = FusionTablesClient(os.environ["REALDEAL_SERVICE_ACCOUNT"],
                                os.environ["REALDEAL_PRIVATE_KEY"], 
                                os.environ["REALDEAL_FUSION_TABLE_ID"])
    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      client.insertRows(properties)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties])
      fout.write(json_str)
      print "%d properties new." % len(properties)