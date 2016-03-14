'''
Created on Mar 11, 2016

@author: pitzer
'''

import json

from luigi_tasks.base_task import RealDealBaseTask
import mortgage

class UpdateMortageData(RealDealBaseTask):
  
  def output(self):
    return self.getLocalFileTarget("properties_with_mortage_data.json")
  
  def run(self):
    with self.input().open() as fin, self.output().open('w') as fout:
      properties_in = json.load(fin)
      properties_out = []
      for prop in properties_in:
        mortage_amount = (1.0 - mortgage.DOWN_PAYMENT) * prop["purchase_price"]
        prop["mortgage_payment"] = mortgage.calcMortgage(mortage_amount)
        properties_out.append(prop)
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties_out])
      fout.write(json_str)