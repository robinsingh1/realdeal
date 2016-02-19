'''
Created on Feb 15, 2016

@author: pitzer
'''


import random
import telnetlib
import time

from scrapy.conf import settings
from scrapy import log
from scrapy.downloadermiddlewares.retry import RetryMiddleware 

class RandomUserAgentMiddleware(object):
  def process_request(self, request, spider):
    ua  = random.choice(settings.get('USER_AGENT_LIST'))
    if ua:
      request.headers.setdefault('User-Agent', ua)
      #this is just to check which user agent is being used for request
      spider.log(
          u'User-Agent: {} {}'.format(request.headers.get('User-Agent'), request),
          level=log.DEBUG
      )


class ProxyMiddleware(object):
  def process_request(self, request, spider):
    request.meta['proxy'] = settings.get('HTTP_PROXY')
    

class RetryChangeProxyMiddleware(RetryMiddleware):
    def _retry(self, request, reason, spider):
        log.msg('Changing proxy')
        tn = telnetlib.Telnet('127.0.0.1', 9051)
        tn.read_until("Escape character is '^]'.", 2)
        tn.write('AUTHENTICATE "267765"\r\n')
        tn.read_until("250 OK", 2)
        tn.write("signal NEWNYM\r\n")
        tn.read_until("250 OK", 2)
        tn.write("quit\r\n")
        tn.close()
        time.sleep(3)
        log.msg('Proxy changed')
        return RetryMiddleware._retry(self, request, reason, spider)