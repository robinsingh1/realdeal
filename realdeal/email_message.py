'''
Created on Apr 19, 2016

@author: pitzer
'''
import sendgrid


class EmailClient(object):

  def __init__(self, sendgrid_api_key):
    self.sg = sendgrid.SendGridClient(sendgrid_api_key)
 
  def send(self, to, subject, html, from_email=None):    
    m = sendgrid.Mail(to=to, subject=subject, html=html, from_email=from_email)
    return self.sg.send(m)
