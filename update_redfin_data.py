'''
Created on Mar 13, 2016

@author: pitzer
'''

import logging
import os

from fusion_tables_client import FusionTablesClient
from redfin_client import RedfinClient, REDFIN_FIELDS

KEY_FIELDS = [
  "rowid",
  "address",
  "city",
  "state",
  "zip",
]

FUSION_FIELDS = KEY_FIELDS + REDFIN_FIELDS

FINAL_STATUS = [
  "not-for-sale",
  "sold",
]

    
def main():
  logging.getLogger().setLevel(logging.INFO)
  fusion_tables = FusionTablesClient(os.environ["REALDEAL_SERVICE_ACCOUNT"],
                                     os.environ["REALDEAL_PRIVATE_KEY"], 
                                     os.environ["REALDEAL_FUSION_TABLE_ID"])
  zillow = RedfinClient()
  
  logging.info("Fetching properties without redfin data from Fusion Table.")
  sql = "SELECT "
  sql += ", ".join(KEY_FIELDS)
  sql += " FROM " + fusion_tables.table_id
  sql += " WHERE status = ''"
  properties = fusion_tables.query(sql)
  
  logging.info("Updating properties.")
  num_updated_properties = 0
  for prop in zillow.updatePropertiesWithRedfinData(properties):
    fusion_tables.updateRow(prop["rowid"], prop)
    num_updated_properties += 1
  
  print "%d properties updated." % num_updated_properties
 
if __name__ == "__main__":
  main()
    


    
    
    
  