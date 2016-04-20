'''
Created on Feb 17, 2016

@author: pitzer
'''

import logging
import os

from fusion_tables_client import FusionTablesClient
from zillow_client import ZillowClient, ZILLOW_FIELDS

    
def main():
  logging.getLogger().setLevel(logging.INFO)
  fusion_tables = FusionTablesClient(table_id=os.environ["REALDEAL_SALES_RECORDS_TABLE_ID"])
  zillow = ZillowClient(os.environ["REALDEAL_ZILLOW_API_KEY"])
  
  logging.info("Fetching properties without Zillow data from Fusion Table.")
  properties = fusion_tables.getRows(columns=ZILLOW_FIELDS,
                                     where={"zillow_id": ""},
                                     limit=500)
  logging.info("Updating properties.")
  num_updated_properties = 0
  for prop, is_updated in zillow.updatePropertiesWithZillowData(properties):
    if is_updated:
      fusion_tables.updateRow(prop["rowid"], prop)
      num_updated_properties += 1
  print "%d properties updated." % num_updated_properties
 
if __name__ == "__main__":
  main()
