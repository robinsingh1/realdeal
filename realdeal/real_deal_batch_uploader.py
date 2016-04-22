'''
Created on Feb 15, 2016

@author: pitzer
'''
import logging
from time import localtime, strftime
               
KEY_COLUMNS = [
  "realtor_property_id",
]

class KeyColumnsNotFound(Exception):
  pass

      
class RealDealBatchUploader(object):
  # Public
  updated_rows = 0
  inserted_rows = 0
  
  # Private
  client = None
  cached_columns = []
  cached_rows = []
  cached_row_index = {}
  
  update_queue = []
  insert_queue = []
  
  def __init__(self, client):
    self.client = client
  
  def getRowKey(self, row):
    return {k:row[k] for k in KEY_COLUMNS}
  
  def getRowKeyHash(self, row):
    return ":".join([row[k] for k in KEY_COLUMNS])
        
  def initializeCache(self, columns):
    self.cached_columns = columns
    logging.info("Fetching current data.")
    self.cached_rows = self.client.getRows(columns=columns)
    logging.info("Building row index.")
    self.cached_row_index = {self.getRowKeyHash(r):r for r in self.cached_rows}
    logging.info("Initialize done.")

  def maybeUpdateRow(self, row):
    row_key_hash = self.getRowKeyHash(row)
    cached_row = self.cached_row_index[row_key_hash]
    
    for key, new_value in row.iteritems():
      current_value = cached_row[key]
      if current_value != new_value:
        row["last_update"] = strftime("%Y-%m-%d %H:%M:%S", localtime())
        self.update_queue.append((cached_row["rowid"], row))
        logging.info("updating row: %s, current value: %s -> new value: %s", 
                     row_key_hash, current_value, new_value)
        return 
      
  def insertRow(self, row):
    row_key_hash = self.getRowKeyHash(row)
    row["created"] = strftime("%Y-%m-%d %H:%M:%S", localtime())
    row["last_update"] = row["created"]
    logging.info("inserting row: %s", row_key_hash)
    self.insert_queue.append(row)
  
  def commit(self):
    if self.insert_queue:
      self.client.insertRows(self.insert_queue)
      self.inserted_rows += len(self.insert_queue)
      self.insert_queue = []
        
    for (row_id, row) in self.update_queue:
      self.client.updateRow(row_id, row)
      self.updated_rows += 1
    self.update_queue = []
        
  def uploadRows(self, rows):
    if not rows:
      return
    
    columns = rows[0].keys()
    columns.append("rowid")
    
    # Check if row data contains key columns. 
    if not all(x in columns for x in KEY_COLUMNS):
      raise KeyColumnsNotFound()
    
    # Check if cache is still valid.
    if columns != self.cached_columns:
      self.initializeCache(columns)
    
    # Queue updates and inserts.
    for row in rows:
      row_key_hash = self.getRowKeyHash(row)
      if row_key_hash in self.cached_row_index:
        self.maybeUpdateRow(row)
      else:
        self.insertRow(row)

    # Commit queues. 
    logging.info("Commiting updates and inserts.")
    self.commit()
    logging.info("Commiting done.")
  
  
    