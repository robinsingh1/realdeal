'''
Created on Mar 13, 2016

@author: pitzer
'''

import logging
import os

from fusion_tables_client import FusionTablesClient
import mortgage

FUSION_FIELDS = [
  "rowid",
  "purchase_price",
  "mortgage_payment",
]

def main():
  logging.getLogger().setLevel(logging.INFO)
  fusion_tables = FusionTablesClient(table_id=os.environ["REALDEAL_FUSION_TABLE_ID"])
  
  logging.info("Fetching properties without mortgage data from Fusion Table.")
  properties = fusion_tables.getRows(columns=FUSION_FIELDS,
                                     where={"mortgage_payment": ""})
  
  logging.info("Updating properties.")
  num_updated_properties = 0
  for prop in properties:
    mortage_amount = (1.0 - mortgage.DOWN_PAYMENT) * prop["purchase_price"]
    prop["mortgage_payment"] = mortgage.calcMortgage(mortage_amount)
    fusion_tables.updateRow(prop["rowid"], prop)
    num_updated_properties += 1
  print "%d properties updated." % num_updated_properties
 
if __name__ == "__main__":
  main()
    