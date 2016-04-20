'''
Created on Apr 19, 2016

@author: pitzer
'''
import json
import locale
import os

from realdeal.email_message import EmailClient
from luigi_tasks.base_task import RealDealBaseTask

PROPERTY_TABLE_FIELDS = [
  'address',
  'city',
  'year_built',
  'bedrooms',
  'bathrooms',
  'building_size',
  'purchase_price',
  'zestimate_amount',
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
]

EMAIL_TO = "ben.pitzer@gmail.com"
EMAIL_FROM = "ben.pitzer@gmail.com"                 

locale.setlocale( locale.LC_ALL, '' )

class EmailDeals(RealDealBaseTask):
  
  def output(self):
    return self.getLocalFileTarget("properties_emailed.json")
  
  def createPropertyTable(self, properties, fields, field_types): 
    html = "<table class=\"tg\">\n"
    
    # Table headers
    html += "<tr>\n"
    for field, field_type in zip(fields, field_types):
      if field_type in ['int', 'float', 'dollar']:
        css_class = 'tg-header-right'
      else:
        css_class = 'tg-header-left'
      html += "<th class=\"%s\">%s</th>\n" % (css_class, field)
    html += "</tr>\n"       
    
    # Table data
    for prop in properties:
      html += "<tr>"
      for field, field_type in zip(fields, field_types):
        if field_type == 'string':
          value = prop.get(field, "")
          css_class = 'tg-data-left'
        elif field_type == 'int':
          value = int(prop.get(field, 0))
          css_class = 'tg-data-right'
        elif field_type == 'float':
          value = float(prop.get(field, 0.0))
          css_class = 'tg-data-right'
        elif field_type == 'dollar':
          value = float(prop.get(field, 0.0))
          if value:
            value = locale.currency(value, grouping=True)
          css_class = 'tg-data-right'   
   
        if not value:
          value = ""
        html += "<td class=\"%s\">%s</td>\n" % (css_class, value)
      html += "</tr>\n"
    html += "</table>\n"
    
    return html
                 
  def run(self):
    client = EmailClient(os.environ["REALDEAL_SENDGRID_API_KEY"])

    with self.input().open() as fin, self.output().open('w') as fout:
      properties = json.load(fin)
      
      html = "<html>\n" 

      html += "<head>\n"
      html += """
        <style type="text/css">
          .tg  {border-collapse:collapse;border-spacing:0;border-color:#ccc;}
          .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#fff;}
          .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#f0f0f0;}
          .tg .tg-header-left{font-weight:bold;vertical-align:top;text-align:left}
          .tg .tg-header-right{font-weight:bold;vertical-align:top;text-align:right}
          .tg .tg-data-left{vertical-align:top;text-align:left}
          .tg .tg-data-right{vertical-align:top;text-align:right}
        </style>
      """
      html += "</head>\n"
      
      html += "<body>\n"
      html += "<div>\n"
      html += "%d new properties found:" % len(properties)
      html += "</div>\n"
      html += "<div>\n"
      html += self.createPropertyTable(properties, 
                                       PROPERTY_TABLE_FIELDS,
                                       PROPERTY_TABLE_FIELD_TYPES)
      html += "</div>\n"
      html += "</body>\n"
      html += "</html>\n"
      
      print html
      subject = "%d new properties found" % len(properties)
      client.send(EMAIL_TO, subject, html, EMAIL_FROM)
      
      json_str = "[%s]" % ",\n".join([json.dumps(p) for p in properties])
      fout.write(json_str)
      print "%d properties emailed." % len(properties)