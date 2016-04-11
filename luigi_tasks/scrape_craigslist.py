'''
Created on Apr 10, 2016

@author: pitzer
'''

import luigi

from luigi_tasks.base_task import RealDealBaseTask
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

class ScrapeCraigslist(RealDealBaseTask):
  spider = luigi.Parameter("craigslist")
  
  def output(self):
    return self.getLocalFileTarget("properties_scraped.json")
                
  def run(self):
    output = self.output()
    output.makedirs()
    settings = get_project_settings()
    settings.set("FEED_FORMAT", "json")
    settings.set("FEED_URI", output.fn)
    settings.set("LOG_FILE", output.fn + ".log")
    settings.set("LOG_LEVEL", "INFO")
    process = CrawlerProcess(settings)
    process.crawl(self.spider)
    process.start() 