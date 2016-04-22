'''
Created on Feb 17, 2016

@author: pitzer
'''

import datetime
import dateutil.parser
import json
import time


class RequestRateLimiter(json.JSONEncoder):
  def __init__(self, max_requests, time_interval, num_requests=0, 
               last_reset=None, state_file=None):
    if max_requests <= 0:
      raise ValueError("max_requests must be positive")
    if time_interval <= 0:
      raise ValueError("time_interval must be positive")
    
    self.__last_reset = last_reset
    self.__max_requests = max_requests
    self.__time_interval = time_interval # seconds
    self.__numrequests = num_requests
    self.__state_file = state_file
  
  @classmethod
  def fromFile(cls, json_file):
    with open(json_file) as f: 
      json_dict = json.load(f)
      last_reset = None
      if "last_reset" in json_dict:
        last_reset = dateutil.parser.parse(json_dict["last_reset"])
        
      return cls(max_requests=json_dict.get("max_requests"), 
                 time_interval=json_dict.get("time_interval"), 
                 num_requests=json_dict.get("numrequests"), 
                 last_reset=last_reset)
  
  def serializeToFile(self, json_file):
    with open(json_file, 'w') as f:
      json_dict = {
        "max_requests": self.__max_requests,
        "time_interval": self.__time_interval,
        "numrequests": self.__numrequests,
      }
      if self.__last_reset:
        json_dict["last_reset"] = self.__last_reset.isoformat()
      json.dump(json_dict, f)
        
  def limit(self, multiplier=1):
    # At the first call, reset the time
    if self.__last_reset == None:
      self.__last_reset = datetime.datetime.now()

    if self.__numrequests + multiplier > self.__max_requests:
      time_delta = datetime.datetime.now() - self.__last_reset
      try:
        time_delta = int(time_delta.total_seconds()) + 1
      except AttributeError:
        time_delta = int((time_delta.microseconds + (time_delta.seconds + time_delta.days * 24 * 3600) * 10**6) / 10**6)

      if time_delta <= self.__time_interval:
        time.sleep(self.__time_interval - time_delta + 1)
        self.__numrequests = 0
        self.__last_reset = datetime.datetime.now()

    self.__numrequests += multiplier
    if self.__state_file:
      self.serializeToFile(self.__state_file)
    return