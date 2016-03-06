'''
Created on Feb 20, 2016

@author: pitzer
'''

import luigi
import os
import time 

from luigi_tasks.base_task import RealDealBaseTask
from luigi_tasks.scrape_realtor import ScrapeRealtor
from luigi_tasks.find_new_properties import FindNewProperties
from luigi_tasks.get_zillow_data import UpdateZillowData
from luigi_tasks.upload_to_fusion_tables import UploadToFusionTables


class RealDealWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data"))
  epoch = luigi.Parameter(
    time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  zillow_api_key = luigi.Parameter(os.environ["REALDEAL_ZILLOW_API_KEY"])
  p12_file = luigi.Parameter(os.environ["REALDEAL_P12_FILE"])
  service_account = luigi.Parameter(os.environ["REALDEAL_SERVICE_ACCOUNT"])
  table_id = luigi.Parameter(os.environ["REALDEAL_FUSION_TABLE_ID"])
  
  def requires(self):
    scrape_realtor_task = ScrapeRealtor(
        base_dir=self.base_dir,
        epoch=self.epoch)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        p12_file=self.p12_file,
        service_account=self.service_account,
        table_id=self.table_id)
    get_zillow_data_task = UpdateZillowData(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        zillow_api_key=os.environ["REALDEAL_ZILLOW_API_KEY"])
    upload_to_fusion_tables_task = UploadToFusionTables(
        upstream_tasks=get_zillow_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        p12_file=self.p12_file,
        service_account=self.service_account,
        table_id=self.table_id)
    return upload_to_fusion_tables_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  luigi.run()
