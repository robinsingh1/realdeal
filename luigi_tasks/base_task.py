'''
Created on Feb 21, 2016

@author: pitzer
'''

import luigi
import os

class RealDealBaseTask(luigi.Task):
  upstream_tasks = luigi.Parameter(default=[])
  base_dir = luigi.Parameter()
  epoch = luigi.Parameter() 
  
  def getEpochDir(self):
    return os.path.join(self.base_dir, self.epoch)
  
  def getLocalFileTarget(self, filename):
    path = os.path.join(self.getEpochDir(), filename)
    return luigi.LocalTarget(path)
  
  def requires(self):
    return self.upstream_tasks