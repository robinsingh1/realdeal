
scrapy

scrapy crawl realtor
scrapy crawl realtor -o properties.json
scrapy crawl craigslist -o properties.json

scrapy shell file://$PWD/hayward.html
 
shub deploy
shub schedule realtor
 
python upload_properties_to_fusion.py

# scrape real estate properties from realtor.com and upload to 
# fusion tables
python realdeal_pipeline.py RealDealWorkflow --local-scheduler
python realdeal_pipeline.py RealDealWorkflow --local-scheduler --epoch=

# scrape rental properties from craigslist and upload to 
# fusion tables
python craigslist_rentals_pipeline.py CraigslistRentalsWorkflow --local-scheduler

INSTALL
=======

pip install pyzillow
pip install python-dateutil

TOR
===

You will find a sample `torrc` file in /usr/local/etc/tor.
It is advisable to edit the sample `torrc` to suit
your own security needs:
  https://www.torproject.org/docs/faq#torrc
After editing the `torrc` you need to restart tor.

To have launchd start tor at login:
  ln -sfv /usr/local/opt/tor/*.plist ~/Library/LaunchAgents
Then to load tor now:
  launchctl load ~/Library/LaunchAgents/homebrew.mxcl.tor.plist
  
  Polipo
To have launchd start polipo at login:
  ln -sfv /usr/local/opt/polipo/*.plist ~/Library/LaunchAgents
Then to load polipo now:
  launchctl load ~/Library/LaunchAgents/homebrew.mxcl.polipo.plist
  
  

 