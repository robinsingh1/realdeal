'''
Created on Apr 9, 2016

@author: pitzer
'''

import scrapy
import urllib

from real_scrapy.items import CraigslistItem

  
class CraigslistSpider(scrapy.Spider):
    name = "craigslist"
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
      for i in range(1, 5):
        start_urls.append(location_url + "&s=" + str(i) + "00")
    
#     start_urls = [
#       "file:///Users/pitzer/Documents/workspace/realdeal/data/craigslist_rentals_hayward.html"
#     ]    

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


#             follow = 'file:///Users/pitzer/Documents/workspace/realdeal/data/craigslist_rentals_hayward_%s.html' % item['craigslist_id']
            #Parse request to follow the posting link into the actual post
            
            request = scrapy.Request(item["link"] , callback=self.parse_item_page)
            request.meta['item'] = item
            yield request

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
        try:
            item["bedrooms"] = int(attr.xpath("span/b/text()")[0].extract())
            bath = attr.xpath("span/b/text()")[1].extract()
            item["building_size"] = int(''.join(attr.xpath("span")[1].xpath("b/text()").extract()))
            if(bath.isdigit()):
                item["bathrooms"] = float(attr.xpath("span/b/text()")[1].extract())
            item["bathrooms"] = float(bath)
        except:
            pass
        postinginfo = response.xpath("//p[@class = 'postinginfo reveal']").xpath("time/@datetime")
        if postinginfo:
          item["posting_date"] = postinginfo[0].extract()
        return item