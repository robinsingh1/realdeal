'''
Created on Feb 17, 2016

@author: pitzer
'''

import logging
import os

from fusion_tables_client import FusionTablesClient
from zillow_client import ZillowClient, ZILLOW_FIELDS

KEY_FIELDS = [
  "rowid",
  "address",
  "city",
  "state",
  "zip",
]

FUSION_FIELDS = KEY_FIELDS + ZILLOW_FIELDS
    
def main():
  logging.getLogger().setLevel(logging.INFO)
  fusion_tables = FusionTablesClient(os.environ["REALDEAL_SERVICE_ACCOUNT"],
                                     os.environ["REALDEAL_PRIVATE_KEY"], 
                                     os.environ["REALDEAL_FUSION_TABLE_ID"])
  zillow = ZillowClient(os.environ["REALDEAL_ZILLOW_API_KEY"])
  
  logging.info("Fetching properties without Zillow data from Fusion Table.")
  properties = fusion_tables.getRows(columns=FUSION_FIELDS,
                                     where={"zillow_id": "24782965"})
  
  logging.info("Updating properties.")
  num_updated_properties = 0
  for prop in zillow.updatePropertiesWithZillowData(properties):
    fusion_tables.updateRow(prop["rowid"], prop)
    num_updated_properties += 1
  
  print "%d properties updated." % num_updated_properties
 
if __name__ == "__main__":
  main()
    


    
    
    
  