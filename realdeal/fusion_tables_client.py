'''
Created on Feb 15, 2016

@author: pitzer
'''

import httplib2
import json
import logging
import os 

from apiclient.discovery import build
from googleapiclient.errors import HttpError
# from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.service_account import ServiceAccountCredentials
from retrying import retry

from realdeal.rate_limiter import RequestRateLimiter

FUSIONT_TABLES_SCOPE='https://www.googleapis.com/auth/fusiontables'
QUOTA_MAX_REQUESTS = 200
QUOTA_TIME_INTERVAL = 100
REQUESTS_PER_WRITE = 5
REQUESTS_PER_READ = 1
RATE_LIMITER_FILE = "fusion_tables_rate_limits.json"
MAX_CONCURRENT_INSERTS = 1

def isRateLimitExceededException(exception):
  """Return True if exception is a rate limit exceeded error."""
  return (isinstance(exception, HttpError) and 
          exception.resp.status == 403 and 
          exception._get_reason().strip() == "Rate Limit Exceeded")

def isInternalErrorException(exception):
  """Return True if exception is a retryable internal error."""
  return (isinstance(exception, HttpError) and 
          exception.resp.status in [502, 503])
  
def isRetryableException(exception):
  """Return True if exception is retryable."""
  return (isInternalErrorException(exception) or
          isRateLimitExceededException(exception))
  
    
class FusionTablesClient(object):
  credentials = None
  service = None
  table_id = None
  
  def __init__(self, table_id,
               service_account=os.environ["REALDEAL_SERVICE_ACCOUNT"], 
               json_keyfile_string=os.environ["REALDEAL_KEYFILE_JSON"]):
    self.table_id = table_id
    keyfile_dict = json.loads(json_keyfile_string)
    self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        keyfile_dict, [FUSIONT_TABLES_SCOPE])
    http = httplib2.Http()
    self.credentials.authorize(http)
    self.service = build('fusiontables', 'v2', http=http)
   
    if os.path.isfile(RATE_LIMITER_FILE):
      self.rate_limiter = RequestRateLimiter.fromFile(RATE_LIMITER_FILE)
    else:
      self.rate_limiter = RequestRateLimiter(max_requests=QUOTA_MAX_REQUESTS, 
                                             time_interval=QUOTA_TIME_INTERVAL,
                                             state_file=RATE_LIMITER_FILE)


  @retry(retry_on_exception=isRetryableException, 
         wait_exponential_multiplier=1000, 
         wait_exponential_max=10000)
  def executeWriteQuery(self, sql, multiplier=REQUESTS_PER_WRITE):
    self.rate_limiter.limit(multiplier=multiplier)
    return self.service.query().sql(sql=sql).execute() 

  @retry(retry_on_exception=isRetryableException, 
         wait_exponential_multiplier=1000, 
         wait_exponential_max=10000)
  def executeReadQuery(self, sql):
    self.rate_limiter.limit(multiplier=REQUESTS_PER_READ)
    return self.service.query().sql(sql=sql).execute() 
  
  def dictValuePad(self, value):
    uvalue = unicode(value) 
    uvalue = uvalue.replace("\\", "\\\\")
    uvalue = uvalue.replace("'", r"\'")
    return "'" + uvalue + "'"
  
  def flattenRowColumnData(self, unflattened_dict):
    if "rows" not in unflattened_dict or "columns" not in unflattened_dict:
      return []
    
    flattened_rows = []
    for row in unflattened_dict["rows"]:
      row_data = {k:v for k,v in zip(unflattened_dict["columns"], row)}
      flattened_rows.append(row_data)
    return flattened_rows
         
  def query(self, sql):
    response = self.executeReadQuery(sql)
    return self.flattenRowColumnData(response)
  
  def getRows(self, 
              columns = ["*"], 
              where={}, 
              order_by=None, 
              limit=None):
    sql = "SELECT "
    sql += ", ".join(columns)
    sql += " FROM " + self.table_id
    conditions = ["%s = %s" % (k, self.dictValuePad(v)) 
                  for k,v in where.iteritems()]
    if conditions:
      sql += " WHERE "
      sql += " AND ".join(conditions)
    if order_by:
      sql += " ORDER BY " + order_by
    if limit:
      sql += " LIMIT %d" % limit
    logging.info("GET ROWS: %s", sql)
    return self.query(sql)
    
  def insertRow(self, row):
    return self.insertRows([row])
  
  def insertRows(self, rows):
    if not rows:
      return
  
    for row_batch in self._batch(rows, MAX_CONCURRENT_INSERTS):
      self._insertRowsInternal(row_batch)
      
  def _batch(self, iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
      yield iterable[ndx:min(ndx + n, l)]
            
  def _insertRowsInternal(self, rows):
    if not rows:
      raise ValueError("rows must contain at least one row")

    sql = ""
    for row in rows:
      sql += "INSERT INTO " + self.table_id
      sql += " ("
      sql += ", ".join(row.keys())
      sql += ") VALUES ("
      sql += ", ".join(map(self.dictValuePad, row.values()))
      sql += ");"
    logging.info("INSERT ROWS: %s", sql)
    response = self.executeWriteQuery(sql, multiplier=REQUESTS_PER_WRITE*len(rows))
    return self.flattenRowColumnData(response)
  
  def updateRow(self, row_id, row):
    sql = "UPDATE " + self.table_id
    sql += " SET "
    column_values = []
    for column, value in row.items():
      if column.lower() == 'rowid':
        continue
      if value == 'NaN':
        continue
      column_values.append("%s = %s" % (column, self.dictValuePad(value)))
    sql += ", ".join(column_values)
    sql += " WHERE ROWID = '%s'" % row_id
    logging.info("UPDATE ROW: %s", sql)
    self.executeWriteQuery(sql)
    return {"rowid": row_id}
  
  def deleteRow(self, row_id):
    sql = "DELETE FROM " + self.table_id
    sql += " WHERE ROWID = '%s'" % row_id
    logging.info("DELETE ROW: %s", sql)
    self.executeWriteQuery(sql)