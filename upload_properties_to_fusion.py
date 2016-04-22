#!/usr/bin/python


import json
import logging
import os

from realdeal.fusion_tables_client import FusionTablesClient
from realdeal.real_deal_batch_uploader import RealDealBatchUploader


if __name__ == "__main__":
#   logging.getLogger().setLevel(logging.INFO)
  client = FusionTablesClient(table_id=os.environ["REALDEAL_FUSION_TABLE_ID"])
  uploader = RealDealBatchUploader(client)
  
  with open('properties.json') as properties_file: 
    properties = json.load(properties_file)
    print "%d properties found." % len(properties)
    uploader.uploadRows(properties)
    print "%d properties updated." % uploader.updated_rows
    print "%d properties new." % uploader.inserted_rows