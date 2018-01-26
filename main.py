#import functions 
from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import def_values as df
import os
import re
from functions import iterate_geojson_job, sat_query_job, download_job, check_files_job, sql_write_and_rename_job, sql_read
import logging

# global variables
footprint_l = {}
error_list = {}
products_new = {}
product_list = []
output_dir_l = []


if __name__ == "__main__":

	api = SentinelAPI(df.username, df.password)
	
	footprint_l = iterate_geojson_job(df.directory, df.directory2, df.sat_limiter)


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
		error_list = check_files_job(products_new, output_dir, api)
		if not error_list:
			sql_write_and_rename_job(products_new, output_dir, foot_list, api, tile_num)
			
