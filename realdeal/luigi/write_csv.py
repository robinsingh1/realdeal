'''
Created on May 7, 2016

@author: pitzer
'''
import csv
import json
import luigi

from realdeal.luigi.base_task import RealDealBaseTask


class WriteCsv(RealDealBaseTask):
  fieldnames = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties.csv")
                    
  def run(self):
    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      dict_writer = csv.DictWriter(fout, self.fieldnames)
      dict_writer.writeheader()
      dict_writer.writerows(properties)