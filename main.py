#import functions 
from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import def_values as df
import os
import re
from functions import iterate_geojson_job, sat_query_job, download_job, check_files_job, sql_write_and_rename_job, sql_read


# global variables
footprint_l = {}
error_list = {}
products_new = {}
product_list = []
output_dir_l = []
has_error = None

if __name__ == "__main__":

	api = SentinelAPI(df.username, df.password)
	
	footprint_l = iterate_geojson_job(df.directory, df.directory2)

	for footprint in footprint_l:

		foot_list = footprint_l[footprint]
		output_dir = foot_list[0]

		sat_and_tile = output_dir.replace(df.directory2,'')
		sat_num, tile_num = re.split('\ |/', sat_and_tile)

		products = sat_query_job(footprint, api, foot_list[1], tile_num)
	
		output_dir = foot_list[0]
		

		# directory query for products to download
		products_new = sql_read(products)
		download_job(products_new, output_dir, api)
		has_error = check_files_job(products_new, output_dir, api)
		if not has_error:
			sql_write_and_rename_job(products_new, output_dir, foot_list, api, tile_num)
			# sql_write(products_new, foot_list, api)
			# rename_job(output_dir, df.directory2, tile_num)

		#sql_read()

		# update directory in case of no error
		# rename files in case of no error

