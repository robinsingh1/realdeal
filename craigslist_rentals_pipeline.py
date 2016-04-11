'''
Created on Feb 20, 2016

@author: pitzer
'''

import luigi
import os
import time 

from luigi_tasks.base_task import RealDealBaseTask
from luigi_tasks.scrape_craigslist import ScrapeCraigslist
from luigi_tasks.find_new_properties import FindNewProperties
from luigi_tasks.update_address_data import UpdateAddressData
from luigi_tasks.upload_to_fusion_tables import UploadToFusionTables


class CraigslistRentalsWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data", "craigslist_rentals"))
  epoch = luigi.Parameter(time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  fusion_service_account = luigi.Parameter(os.environ["REALDEAL_SERVICE_ACCOUNT"])
  fusion_private_key = luigi.Parameter(os.environ["REALDEAL_PRIVATE_KEY"])
  fusion_table_id = luigi.Parameter(os.environ["REALDEAL_RENTALS_TABLE_ID"])
  
  def requires(self):
    scrape_realtor_task = ScrapeCraigslist(
        base_dir=self.base_dir,
        epoch=self.epoch)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        key_columns="title,bathrooms,bedrooms,price",
        fusion_service_account = self.fusion_service_account,
        fusion_private_key = self.fusion_private_key,
        fusion_table_id = self.fusion_table_id)
    update_address_data_task = UpdateAddressData(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    upload_to_fusion_tables_task = UploadToFusionTables(
        upstream_tasks=update_address_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        fusion_service_account = self.fusion_service_account,
        fusion_private_key = self.fusion_private_key,
        fusion_table_id = self.fusion_table_id)
    return upload_to_fusion_tables_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  luigi.run()
