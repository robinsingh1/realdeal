#!/bin/bash

export REALDEAL_EMAIL_LIST="realdeal-bay-area-single-family@googlegroups.com"
export REALDEAL_FUSION_TABLE_ID="1GZUfnE30OOk7e-hpJ5B80qGYGCkv9Rleqnkt8uR2"
export REALTOR_SPIDER_BASE_URL="http://www.realtor.com/realestateandhomes-search/{location}/type-single-family-home/price-na-700000/shw-all?pgsz=500"

python realtor_pipeline.py RealDealWorkflow --local-scheduler