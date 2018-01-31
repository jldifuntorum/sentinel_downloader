#import functions 
from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt, SentinelAPIError
import def_values_for_hpc as df
import os
import re
from functions import iterate_geojson_job, sat_query_job, download_job, check_files_job, sql_write_and_rename_job, sql_read, database_init
import logging
import sys
import traceback

# global variables
footprint_l = {}
error_list = {}
products_new = {}


if __name__ == "__main__":

	try:

		try:
			if sys.argv[1] == '-r':
				database_init()
		except IndexError as e:
			pass

		api = SentinelAPI(df.username, df.password)

			
		footprint_l = iterate_geojson_job(df.directory, df.directory2, df.sat_limiter)


		for footprint in footprint_l:

			foot_list = footprint_l[footprint]
			output_dir = foot_list[0]

			products = sat_query_job(footprint, api, foot_list[1], foot_list[2])
		
			# directory query for products to download
			products_new = sql_read(products, output_dir, foot_list)
			download_job(products_new, output_dir, api)
			error_list = check_files_job(products_new, output_dir, api)
			if not error_list:
				sql_write_and_rename_job(products_new, output_dir, foot_list, api, foot_list[2])

	except:

		database_init()
		print(traceback.format_exc())

			
