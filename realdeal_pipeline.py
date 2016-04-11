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
from luigi_tasks.update_redfin_data import UpdateRedfinData
from luigi_tasks.update_mortage_data import UpdateMortageData
from luigi_tasks.update_zillow_data import UpdateZillowData
from luigi_tasks.upload_to_fusion_tables import UploadToFusionTables


class RealDealWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data"))
  epoch = luigi.Parameter(
    time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  
  fusion_service_account = luigi.Parameter(os.environ["REALDEAL_SERVICE_ACCOUNT"])
  fusion_private_key = luigi.Parameter(os.environ["REALDEAL_PRIVATE_KEY"])
  fusion_table_id = luigi.Parameter(os.environ["REALDEAL_FUSION_TABLE_ID"])
  
  def requires(self):
    scrape_realtor_task = ScrapeRealtor(
        base_dir=self.base_dir,
        epoch=self.epoch)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        fusion_service_account = os.environ["REALDEAL_SERVICE_ACCOUNT"],
        fusion_private_key = os.environ["REALDEAL_PRIVATE_KEY"],
        fusion_table_id = os.environ["REALDEAL_FUSION_TABLE_ID"])
    get_mortgage_data_task = UpdateMortageData(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
#     get_redfin_data_task = UpdateRedfinData(
#         upstream_tasks=get_mortgage_data_task,
#         base_dir=self.base_dir,
#         epoch=self.epoch)
    get_zillow_data_task = UpdateZillowData(
        upstream_tasks=get_mortgage_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    upload_to_fusion_tables_task = UploadToFusionTables(
        upstream_tasks=get_zillow_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    return upload_to_fusion_tables_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  luigi.run()
