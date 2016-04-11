#!/usr/bin/python
import os

from fusion_tables_client import FusionTablesClient


if __name__ == "__main__":
  client = FusionTablesClient(os.environ["REALDEAL_SERVICE_ACCOUNT"],
                              os.environ["REALDEAL_PRIVATE_KEY"], 
                              os.environ["REALDEAL_FUSION_TABLE_ID"])
  rows = client.getRows(columns=["rowid", "zillow_id"], order_by="zillow_id")
  dupe_rowids = []
  prev_zillow_id = ""
  for row in rows:
    current_zillow_id = row["zillow_id"]
    if current_zillow_id and current_zillow_id == prev_zillow_id:
      dupe_rowids.append(row["rowid"])
    else:
      prev_zillow_id = current_zillow_id
  
  for rowid in dupe_rowids:
    client.deleteRow(rowid)  
  
  print "Removed %d dupes." % len(dupe_rowids)
