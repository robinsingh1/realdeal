'''
Created on May 7, 2016

@author: pitzer
'''

import logging
import luigi
import os
import time 

from realdeal.luigi.base_task import RealDealBaseTask
from realdeal.luigi.scrape_redfin import ScrapeRedfin
from realdeal.luigi.find_new_properties import FindNewProperties
from realdeal.luigi.write_csv import WriteCsv
from realdeal.luigi.update_address_data import UpdateAddressData
from realdeal.luigi.upload_to_fusion_tables import UploadToFusionTables


SACRAMENTO_REGIONS = [
  16409,    # Sacramento
  20818,    # West Sacramento
  22674,    # Florin
  21251,    # Arden-Arcade
  25389,    # Rosemont
  25247,    # Rio Linda
  35837,    # Elerta
  5672,     # Elk Grove
  26666,    # Vineyard
  25389,    # Rosemont
]
  
DEFAULT_REGIONS = [
  5401,     # East Palo Alto
  11961,    # Menlo Park
  24616,    # North Fairoaks
  820,      # Atherton
  8439,     # Hayward
  15525,    # Redwood City
  6671,     # Fremont
  17447,    # San Leandro
  21841,    # Castro Valley
  14986,    # Pleasenton
  5159,     # Dublin
  10683,    # Livermore
  17519,    # San Ramon
  25747,    # San Lorenzo
  21280,    # Ashland
  21906,    # Cherryland
  13111,    # Newark
  20321,    # Union City 
  12204,    # Milpitas
  1590,     # Berkeley
  13654,    # Oakland
#   117,      # Alameda
  17420,    # San Jose
  2673,     # Campbell
  17960,    # Saratoga
  4561,     # Cupertino
  11234,    # Los Gatos
  17675,    # Santa Clara
  19457,    # Sunnyvale
  12739,    # Mountain View
  11018,    # Los Altos
  11022,    # Los Altos Hills
  14325,    # Palo Alto
  15117,    # Portola Valley
  20966,    # Woodside
  16687,    # San Carlos
  1362,     # Belmont
  17490,    # San Mateo
  6524,     # Foster City
  2350,     # Burlingame
  12130,    # Millbrae
  16671,    # San Bruno
  4150,     # Concord
  24837,    # Pacheco
  11767,    # Martinez
  21367,    # Bay Point
  14890,    # Pittsburg
  588,      # Antioch
  2112,     # Brentwood
  13671,    # Oakley
  23627,    # Knightsen
  14983,    # Pleasant Hill 
  20635,    # Walnut Creek
  9927,     # Lafayette
  14057,    # Orinda
  12596,    # Moraga
  148,      # Albany
  23580,    # Kensington
  5618,     # El Cerrito
  15629,    # Richmond
  17500,    # San Pablo
  34009,    # North Richmond
  25374,    # Rollingwood
  26414,    # Tara Hills
  31127,    # Bayview
  8519,     # Hercules
  14852,    # Pinole
  25362,    # Rodeo
  22185,    # Crockett
  12625,    # Morgan Hill
  25759,    # San Martin
  7521,     # Gilroy
  20371,    # Valejo
  1415,     # Benicia
  419,      # American Canyon
  5980,     # Fairfield
  19156,   # Suisun City
  23142,    # Green Valley
  20361,    # Vacaville
  28232,    # Vacaville-Junction
  5007,     # Dixon
  20935,    # Winters
  4690,     # Davis
  20957,    # Woodland
  17680,    # Santa Cruz
  2848,     # Capitola
  23911,    # Live Oak
  26605,    # Twin Lakes
  34284,    # Pleasure Point
  26051,    # Soquel
  18042,    # Scotts Valley
  22628,    # Felton
  21242,    # Aptos
  25244,    # Rio del Mar
  20685,    # Watsonville
  23478,    # Interlaken
  21197,    # Amesti
  24448,    # Moss Landing
  21843,    # Castroville
  11675,    # Marina
  16477,    # Salinas
  18079,    # Seaside
  14191,    # Pacific Grove
  12514,    # Monterey
  22266,    # Del Monte Forrest
  17518,    # San Rafael
  10318,    # Larkspur
  12192,    # Mill Valley
  4289,     # Corte Madera
  23583,    # Kentfield
  16157,    # Ross
  16526,    # San Anselmo
  5976,     # Fairfax
  26806,    # Woodacre
  23666,    # Lagunitas Forest-Knolls
  25679,    # San Geromino
  27814,    # Paradise Cay
  26241,    # Strawberry
  19818,    # Tiburon
  27769,    # Belvedere-Tiburon
  27802,    # Marin City
  17978,    # Sausalito
  26398,    # Tamalpais-Homestead-Valley
  24473,    # Muir Beach
  26150,    # Stinson Beach
  25876,    # Santa Venetia
  14699,    # Petaluma
  13547,    # Novato
  22539,    # El Sobrante
  22308,    # Diablo
  21136,    # Alamo
  14104,    # Alamo Oaks
  4658,     # Danville
  14181,    # Pacifica
  24380,    # Montara
  24444,    # Moss Beach
  22492,    # El Granada
  8105,     # Half Moon Bay
  18672,    # South San Francisco
  4636,     # Daly City
  21618,    # Broadmoor
  3819,     # Colma
  2158,     # Brisbane
  17151,    # San Francisco
]

FUSION_FIELDS = [
  'status',
  'sold_date',
  'address',
  'city',
  'state',
  'zip',
  'mls_id',
  'price',
  'building_size',
  'lot_size',
  'year_built',
  'bedrooms',
  'bathrooms',
  'redfin_property_id',
  'redfin_listing_id',
  'redfin_url',
  'latitude',
  'longitude',
  'zillow_id',
  'zillow_url',
  'home_type',
  'tax_year',
  'tax_value',
  'last_sold_date',
  'last_sold_price',
  'zestimate_amount',
  'zestimate_last_updated',
  'zestimate_value_change',
  'zestimate_valuation_range_high',
  'zestimate_valuation_range_low',
  'zestimate_percentile',
  'rentzestimate_amount',
  'rentzestimate_last_updated',
  'rentzestimate_value_change',
  'rentzestimate_valuation_range_high',
  'rentzestimate_valuation_range_low',
  'region_name',
  'region_type',
  'last_update',
  'zillow_last_update',
  'created'
]


class RedfinSalesRecordsWorkflow(RealDealBaseTask):
  base_dir = luigi.Parameter(os.path.join(os.getcwd(), "data", "redfin"))
  epoch = luigi.Parameter(time.strftime("%Y%m%d-%H%M%S", time.localtime()))
  fusion_table_id = luigi.Parameter(os.environ["REALDEAL_SALES_RECORDS_TABLE_ID"])
  regions = luigi.Parameter(DEFAULT_REGIONS)
                                    
  def requires(self):
    scrape_realtor_task = ScrapeRedfin(
        base_dir=self.base_dir,
        epoch=self.epoch,
        regions=self.regions)
    find_new_properties_task = FindNewProperties(
        upstream_tasks=scrape_realtor_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        key_columns="redfin_property_id",
        fusion_table_id = self.fusion_table_id)
    write_csv_task = WriteCsv(
        upstream_tasks=find_new_properties_task,
        base_dir=self.base_dir,
        epoch=self.epoch,
        fieldnames=FUSION_FIELDS)
#     return upload_to_fusion_tables_task
#     upload_to_fusion_tables_task = UploadToFusionTables(
#         upstream_tasks=update_address_data_task,
#         base_dir=self.base_dir,
#         epoch=self.epoch,
#         fusion_table_id = self.fusion_table_id)
#     return upload_to_fusion_tables_task
    return write_csv_task
  
  def output(self):
    return self.getLocalFileTarget("workflow_complete")
  
  def run(self):
    with self.output().open('w') as outfile:
      outfile.write('OK')
 
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  luigi.run()
