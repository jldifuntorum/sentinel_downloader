from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import os 
import schedule
import time, datetime
import sys
import pandas as pd
import glob 
import re
import sqlite3
import def_values as df

#Function Declarations

#disregard hidden files in iteration
def listdir_nohidden(path):
    return glob.glob(os.path.join(path, '*'))


# Iterate over all geojson
def iterate_geojson_job(directory, directory2):

	footprint_list = {}
	#scene_dir_list = []
	# Iterate over all directories in the GeoJSON folder
	for subdir in listdir_nohidden(directory):
	    
	    # Iterate over all GeoJSON files 

	    for filename in os.listdir(subdir):


	        if filename.endswith(".geojson"):

	        	
				footprint = os.path.join(subdir, filename)
				#footprint_list.append(footprint)


				# extract the satellite number sub-directory
				subdir_min = subdir.replace(directory, '')

				# extract the areacode
				filename_min = filename.replace('.geojson', '')

					# define the directory where the downloaded data will be saved
				if re.search('s1a', subdir_min, re.IGNORECASE):
				    scene_dir = os.path.join(directory2,'S1A', 'S1A_' + filename_min)
				elif re.search('s1b', subdir_min, re.IGNORECASE): 
				    scene_dir = os.path.join(directory2,'S1B', 'S1B_' + filename_min)


				# Create directories if GeoJSON folder does not exist
				if not os.path.exists(scene_dir):
				    os.makedirs(scene_dir)
				    print(scene_dir + " folder created.") 

				#scene_dir_list.append(scene_dir)
				footprint_list[footprint] = scene_dir 

				

	#return (footprint_list, scene_dir_list)
	return footprint_list


# Call SentinelSat API for querying satellite data
def sat_query_job(footprint, api):

	footprint_rd = geojson_to_wkt(read_geojson(footprint))

	raw_query = ''
	if df.file_name is not None:
	    raw_query = raw_query + 'filename:%s AND ' % df.file_name
	if df.product_type is not None:
	    raw_query = raw_query + 'producttype:%s AND ' % df.product_type
	if df.platform_name is not None:
	    raw_query = raw_query + 'platformname:%s AND ' % df.platform_name
	if df.orbit_direction is not None:
	    raw_query = raw_query + 'orbitdirection:%s AND ' % df.orbit_direction
	if df.polarisation_mode is not None:
	    raw_query = raw_query + 'polarisationmode:%s AND ' % df.polarisation_mode
	if df.cloud_cover_percentage is not None:
	    raw_query = raw_query + 'cloudcoverpercentage:%s AND ' % df.cloud_cover_percentage
	if df.sensor_operational_mode is not None:
	    raw_query = raw_query + 'sensoroperationalmode:%s AND ' % df.sensor_operational_mode
	raw_query = raw_query[:-5]

	# search by polygon, time, and SciHub query keywords
	products = api.query(footprint_rd, date = (df.start_date, df.end_date), raw = raw_query)

	# print results from the search
	if df.printProducts:
	    print "%d products found for " % len(products) + footprint
	    for product in products:
	        print product

	return products

def sqldb_access_job(products):

	# write to database
	products_new = {}
	if df.writeToDB:
	    conn = sqlite3.connect('sentinel')
	    c = conn.cursor()
	    products_new = dict(products)
	    for product in products:
	        c.execute('SELECT * FROM downloads where productid = ?', (product,)) 
	        temp = c.fetchone()
	        if temp is None:
	            if products[product]['filename'].startswith('S1A'):
	                attr_platformname = 'Sentinel-1A'
	            elif products[product]['filename'].startswith('S1B'):
	                attr_platformname = 'Sentinel-1B'
	            attr_dateacquired = str(products[product]['beginposition']).split()[0]
	            attr_producttype = products[product]['producttype']
	            attr_orbitdirection = products[product]['orbitdirection']
	            attr_polarisationmode = products[product]['polarisationmode']
	            attr_sensoropmode = products[product]['sensoroperationalmode']
	            attr_productid = product
	            attr_datedownloaded = str(datetime.today()).split()[0]
	            parameters = (attr_platformname, areacode, attr_dateacquired,
	                          attr_producttype, attr_orbitdirection,
	                          attr_polarisationmode, attr_sensoropmode,
	                          attr_productid, attr_datedownloaded)
	            c.execute("INSERT INTO downloads VALUES (NULL,?,?,?,?,?,?,?,?,?)", parameters)
	    	else:
	        	products_new.pop(product)
	        	print(product + "_popped")
	        
		conn.commit()
		conn.close()

	return products_new

def download_job(products, output_dir, api):

	# download all results from the search
	if df.downloadProducts:
	    api.download_all(products, output_dir)


	products_df = api.to_dataframe(products)

	now_time = datetime.datetime.now()

	products_df.to_csv(str(now_time)+'.csv')
	print(str(now_time)+'.csv created at ' + output_dir)
