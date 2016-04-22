#!/usr/bin/python
import logging
import os

from realdeal.fusion_tables_client import FusionTablesClient


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  client = FusionTablesClient(table_id=os.environ["REALDEAL_RENTALS_TABLE_ID"])
  rows = client.getRows(columns=["rowid", "craigslist_id"], order_by="created DESC")
  dupe_rowids = []
  visited_ids = set()
  
  for row in rows:
    current_id = row["craigslist_id"]
    if not current_id:
      continue
    
    if current_id in visited_ids:
      dupe_rowids.append(row["rowid"])
    else:
      visited_ids.add(current_id)
  
  print "Removing %d dupes." % len(dupe_rowids)
  for rowid in dupe_rowids:
    client.deleteRow(rowid)  
  
  print "Removed %d dupes." % len(dupe_rowids)
