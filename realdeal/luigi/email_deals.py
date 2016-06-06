'''
Created on Apr 19, 2016

@author: pitzer
'''
import json
import locale
import luigi
import os

from realdeal.email_message import EmailClient
from realdeal.luigi.base_task import RealDealBaseTask

PROPERTY_TABLE_FIELDS = [
  'address',
  'city',
  'year_built',
  'bedrooms',
  'bathrooms',
  'building_size',
  'purchase_price',
  'zestimate_amount',
  'zillow_url',
]

PROPERTY_TABLE_FIELD_TYPES = [
  'string',
  'string',
  'int',
  'int',
  'float',
  'int',
  'dollar',
  'dollar',       
  'link',                 
]

EMAIL_FROM = "ben.pitzer@gmail.com"                 

locale.setlocale( locale.LC_ALL, '' )

class EmailDeals(RealDealBaseTask):
  email_to = luigi.Parameter()
  
  def output(self):
    return self.getLocalFileTarget("properties_emailed.json")
  
  def createPropertyTable(self, properties, fields, field_types): 
    html = "<table style=\"border-collapse:collapse;border-spacing:0;border-color:#ccc\">\n"
 
    # Table headers
    html += "<tr>\n"
    for field, field_type in zip(fields, field_types):
      if field_type in ['int', 'float', 'dollar']:
        text_align = 'right'
      else:
        text_align = 'left'
      html += "<th style=\"font-family:Arial, sans-serif;font-size:14px;font-weight:bold;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#f0f0f0;vertical-align:top;text-align:%s\">%s</th>\n" % (text_align, field)
    html += "</tr>\n"       
    
    # Table data
    for prop in properties:
      html += "<tr>"
      for field, field_type in zip(fields, field_types):
        if field_type == 'string':
          value = prop.get(field, "")
          text_align = 'left'
        elif field_type == 'int':
          value = int(prop.get(field, 0))
          text_align = 'right'
        elif field_type == 'float':
          value = float(prop.get(field, 0.0))
          text_align = 'right'
        elif field_type == 'dollar':
          value = float(prop.get(field, 0.0))
          if value:
            value = locale.currency(value, grouping=True)
          text_align = 'right'   
        elif field_type == 'link':
          value = prop.get(field, "")
          if value:
            value = "<a href=\"%s\">link</a>" % value
          text_align = 'left' 
          
        if not value:
          value = ""
        html += "<td style=\"font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#fff;vertical-align:top;text-align:%s\">%s</td>\n" % (text_align, value)
      html += "</tr>\n"
    html += "</table>\n"
    
    return html
                 
  def run(self):
    client = EmailClient(os.environ["REALDEAL_SENDGRID_API_KEY"])

    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      
      html = ""
      html += "<div>\n"
      html += "Hello, I found the following properties for you:"
      html += "</div>\n"
      html += "<div>\n"
      html += self.createPropertyTable(properties, 
                                       PROPERTY_TABLE_FIELDS,
                                       PROPERTY_TABLE_FIELD_TYPES)
      html += "</div>\n"

      subject = "Found %d new properties" % len(properties)
      client.send(self.email_to, subject, html, EMAIL_FROM)
      
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties])
      fout.write(json_str)
      print "%d properties emailed." % len(properties)