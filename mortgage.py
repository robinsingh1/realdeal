'''
Created on Mar 13, 2016

@author: pitzer
'''

import math


DOWN_PAYMENT = 0.2
INTEREST = 4.0
YEARS = 30


def calcMortgage(principal, interest=INTEREST, years=YEARS):
  '''
  given mortgage loan principal, interest(%) and years to pay
  calculate and return monthly payment amount
  '''
  # monthly rate from annual percentage rate
  interest_rate = interest/(100 * 12)
  # total number of payments
  payment_num = years * 12
  # calculate monthly payment
  payment = principal * \
      (interest_rate/(1-math.pow((1+interest_rate), (-payment_num))))
  return payment