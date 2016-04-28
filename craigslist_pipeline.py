'''
Created on Feb 20, 2016

@author: pitzer
'''
import logging
import luigi
import os
import time 

from realdeal.luigi.base_task import RealDealBaseTask
from realdeal.luigi.scrape_craigslist import ScrapeCraigslist
from realdeal.luigi.find_new_properties import FindNewProperties
from realdeal.luigi.update_address_data import UpdateAddressData
from realdeal.luigi.upload_to_fusion_tables import UploadToFusionTables


class CraigslistRentalsWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data", "craigslist"))
  epoch = luigi.Parameter(time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  fusion_table_id = luigi.Parameter(os.environ["REALDEAL_RENTALS_TABLE_ID"])
  
  def requires(self):
    scrape_realtor_task = ScrapeCraigslist(
        base_dir=self.base_dir,
        epoch=self.epoch)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        key_columns="craigslist_id",
        fusion_table_id = self.fusion_table_id)
    update_address_data_task = UpdateAddressData(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    upload_to_fusion_tables_task = UploadToFusionTables(
        upstream_tasks=update_address_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        fusion_table_id = self.fusion_table_id)
    return upload_to_fusion_tables_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  luigi.run()
