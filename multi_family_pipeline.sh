#!/bin/bash

export REALDEAL_EMAIL_LIST="realdeal-bay-area-multi-family@googlegroups.com"
export REALDEAL_EMAIL_SUBJECT="Found {num_properties} multi-family properties"
export REALDEAL_FUSION_TABLE_ID="164VzLJFRBsA2Q_xsSmKhb0YteCVQ-AwHOc9rWZqt"
export REALDEAL_RENTALS_TABLE_ID="124OgEgoV-KB6M9BhOqZwx93jcI2-HGknpLCL_qbu"

export REALTOR_SPIDER_BASE_URL="http://www.realtor.com/realestateandhomes-search/{location}/type-multi-family-home/price-na-1000000/shw-all?pgsz=500"
export CRAIGSLIST_SPIDER_BASE_URL="https://sfbay.craigslist.org/search/apa?query={location}&bedrooms=1&bedrooms=2&housing_type=1&housing_type=4"


python realtor_pipeline.py RealDealWorkflow --local-scheduler
python craigslist_pipeline.py CraigslistRentalsWorkflow --local-scheduler