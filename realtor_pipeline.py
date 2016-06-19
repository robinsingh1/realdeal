'''
Created on Feb 20, 2016

@author: pitzer
'''

import luigi
import os
import time 

from realdeal.luigi.base_task import RealDealBaseTask
from realdeal.luigi.scrape_realtor import ScrapeRealtor
from realdeal.luigi.find_new_properties import FindNewProperties
from realdeal.luigi.update_mortage_data import UpdateMortageData
from realdeal.luigi.update_zillow_data import UpdateZillowData
from realdeal.luigi.upload_to_fusion_tables import UploadToFusionTables
from realdeal.luigi.email_deals import EmailDeals


class RealDealWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data", "realtor"))
  epoch = luigi.Parameter(
    time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  
  fusion_table_id = luigi.Parameter(os.environ["REALDEAL_FUSION_TABLE_ID"])
  email_list = luigi.Parameter(os.getenv("REALDEAL_EMAIL_LIST", ""))
  
  def requires(self):
    scrape_realtor_task = ScrapeRealtor(
        base_dir=self.base_dir,
        epoch=self.epoch)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        key_columns="realtor_property_id",
        fusion_table_id = self.fusion_table_id)
    get_mortgage_data_task = UpdateMortageData(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    get_zillow_data_task = UpdateZillowData(
        upstream_tasks=get_mortgage_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch)
    upload_to_fusion_tables_task = UploadToFusionTables(
        upstream_tasks=get_zillow_data_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        fusion_table_id = self.fusion_table_id)
    if self.email_list:
      email_task = EmailDeals(
          upstream_tasks=upload_to_fusion_tables_task,
          base_dir=self.base_dir,
          epoch=self.epoch,
          email_to=self.email_list)
      return email_task
    else:
      return upload_to_fusion_tables_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  luigi.run()
