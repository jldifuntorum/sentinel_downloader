from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt

import def_values as df
import os

from functions import iterate_geojson_job, sat_query_job, download_job
#import functions 

if __name__ == "__main__":

	api = SentinelAPI(df.username, df.password)
	footprint_l = {}
	footprint_l = iterate_geojson_job(df.directory, df.directory2)

	for footprint in footprint_l:
		#print (footprint)
		products = sat_query_job(footprint, api)
		output_dir = footprint_l[footprint]
		os.chdir(output_dir)
		#print('down_dir is ' + output_dir)
		download_job(products, output_dir, api)
