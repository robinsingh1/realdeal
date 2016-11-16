#!/bin/bash

export REALDEAL_EMAIL_LIST="realdeal-bay-area-single-family@googlegroups.com"
export REALDEAL_EMAIL_SUBJECT="Found {num_properties} single-family properties"
export REALDEAL_FUSION_TABLE_ID="1GZUfnE30OOk7e-hpJ5B80qGYGCkv9Rleqnkt8uR2"
export REALDEAL_RENTALS_TABLE_ID="14lPUMeCKenDAeTerTBc4BMqIDzaXNwFGcoVYNqP9"

export REALTOR_SPIDER_BASE_URL="http://www.realtor.com/realestateandhomes-search/{location}/type-single-family-home/price-na-800000/shw-all?pgsz=500"
export CRAIGSLIST_SPIDER_BASE_URL="https://sfbay.craigslist.org/search/apa?query={location}&bedrooms=2&housing_type=6"


python realtor_pipeline.py RealDealWorkflow --local-scheduler
python craigslist_pipeline.py CraigslistRentalsWorkflow --local-scheduler