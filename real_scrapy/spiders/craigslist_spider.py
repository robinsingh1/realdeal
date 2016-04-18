'''
Created on Apr 9, 2016

@author: pitzer
'''

import scrapy
import urllib

from real_scrapy.items import CraigslistItem

  
class CraigslistSpider(scrapy.Spider):
    name = "craigslist"
    debug = False
    if not debug:
      allowed_domains = ["craigslist.org"]
    
    locations = [
#       "Alameda",
#       "Ashland",
#       "Atherton",
#       "Belmont",
      "Berkeley",
#       "Burlingame",
#       "Campbell",
      "Castro Valley",
#       "Cherryland",
#       "Cupertino",
#       "Dublin",
      "East Palo Alto",
#       "Foster City",
#       "Fremont",
      "Hayward",
#       "Livermore",
#       "Los Altos Hills",
#       "Los Altos",
#       "Los Gatos",
      "Menlo Park",
#       "Milbrae",
#       "Milpitas",
#       "Mountain View",
#       "Newark",
#       "North Fairoaks",
      "Oakland",
#       "Palo Alto",
#       "Pleasenton",
#       "Portola Valley",
      "Redwood City",
#       "San Bruno",
#       "San Carlos",
      "San Jose",
      "San Leandro",
      "San Lorenzo",
#       "San Mateo",
#       "San Ramon",
#       "Santa Clara",
#       "Saratoga",
#       "Sunnyvale",
      "Union City",
#       "Woodside",
    ]
    base_url = "https://sfbay.craigslist.org/search/apa?query={location}&bedrooms=1&housing_type=6"
    start_urls = []
    for l in locations:
      # scrape first page
      location_url = base_url.format(location=urllib.quote_plus(l))
      start_urls.append(location_url)
      # scrape subsequent pages
#       for i in range(1, 5):
#         start_urls.append(location_url + "&s=" + str(i) + "00")
    
    if debug:
      start_urls = [
        "file:///Users/pitzer/Documents/workspace/realdeal/data/craigslist_rentals_hayward.html"
      ]    

    def parse(self, response):
#         with open('data/craigslist_rentals_hayward.html', 'wb') as f:
#           f.write(response.body)

        #find all postings
        postings = response.xpath(".//p")
        #loop through the postings
        for i in range(0, len(postings)-1):
#         for i in range(0, 4):
            item = CraigslistItem()
            #grab craiglist apartment listing ID
            item["craigslist_id"] = int(''.join(postings[i].xpath("@data-pid").extract()))
            temp = postings[i].xpath("span[@class='txt']")
            info = temp.xpath("span[@class='pl']")
            #title of posting
            item["title"] = ''.join(info.xpath(".//span[@id='titletextonly']/text()").extract())
#             import pdb; pdb.set_trace()
            #pre-processing for getting the price in the right format
            price = ''.join(temp.xpath("span")[2].xpath("span[@class='price']").xpath("text()").extract())
            item["price"] = price.replace("$","")
            item["link"] = "http://sfbay.craigslist.org" + ''.join(info.xpath("a/@href").extract())


            if self.debug:
              item["link"] = 'file:///Users/pitzer/Documents/workspace/realdeal/data/craigslist_rentals_hayward_5530420563.html'
           
            item["link"] = 'http://sfbay.craigslist.org/eby/apa/5542791384.html'
            #Parse request to follow the posting link into the actual post
            request = scrapy.Request(item["link"] , callback=self.parse_item_page)
            request.meta['item'] = item
            yield request

    def isFloat(self, value):
      try:
        float(value)
        return True
      except:
        return False
      
    def isInt(self, value):
      try:
        int(value)
        return True
      except:
        return False
      
    #Parsing method to grab items from inside the individual postings
    def parse_item_page(self, response):
        item = response.meta["item"]
        maplocation = response.xpath("//div[contains(@id,'map')]")
        latitude = ''.join(maplocation.xpath('@data-latitude').extract())
        longitude = ''.join(maplocation.xpath('@data-longitude').extract())
        if latitude:
            item['latitude'] = float(latitude)
        if longitude:
            item['longitude'] = float(longitude)
            
        attr = response.xpath("//p[@class='attrgroup']")
        
        
        # Extract bedrooms and bathrooms.
        bed_bath_selector = attr.xpath("span/b/text()")
#         import pdb; pdb.set_trace()
        if len(bed_bath_selector) > 0:
          bedrooms = bed_bath_selector[0].extract()
          if self.isInt(bedrooms):
            item["bedrooms"] = int(bedrooms)
        if len(bed_bath_selector) > 1:
          bathrooms = bed_bath_selector[1].extract()
          if self.isFloat(bathrooms):
              item["bathrooms"] = float(bathrooms)
                
        # Extract the building size.
        building_size_selector = attr.xpath("span")
        if len(building_size_selector) > 1:
          building_size = ''.join(building_size_selector[1].xpath("b/text()").extract())
          if(building_size.isdigit()):
            item["building_size"] = int(building_size)
        
        # Extract the posting date.
        postinginfo = response.xpath("//p[@class = 'postinginfo reveal']").xpath("time/@datetime")
        if postinginfo:
          item["posting_date"] = postinginfo[0].extract()
        return item