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
import csv
from collections import defaultdict

#Function Declarations

#disregard hidden files in iteration
def listdir_nohidden(path):
    return glob.glob(os.path.join(path, '*'))

def iterate_files(files, satnum, directory2, root):

	footprint_list = defaultdict(list)

	for filename in files:
		if filename.endswith('.geojson'):
			footprint = os.path.join(root, filename)
			tile_num = satnum + '_' + filename.replace('.geojson','')
			scene_dir = os.path.join(directory2, satnum, tile_num)

			# Create directories if GeoJSON folder does not exist
			if not os.path.exists(scene_dir):
			    os.makedirs(scene_dir)
			    print(scene_dir + " folder created.") 

			footprint_list[footprint].append(scene_dir)
			footprint_list[footprint].append(satnum) 
			footprint_list[footprint].append(tile_num)
			
	return footprint_list


# Iterate over all geojson
def iterate_geojson_job(directory, directory2, sat_limiter):

	foot_list = defaultdict(list)

	for root, dirs, files in os.walk(directory):

		if sat_limiter == 'S1A_only' and re.search('s1a', root, re.IGNORECASE):
			foot_list = iterate_files(files, 'S1A', directory2, root)
		elif sat_limiter == 'S1B_only' and re.search('s1b', root, re.IGNORECASE):
			foot_list = iterate_files(files, 'S1B', directory2, root)
		elif sat_limiter == 'both':
			if re.search('s1a', root, re.IGNORECASE):
				foot_list.update(iterate_files(files, 'S1A', directory2, root))
			elif re.search('s1b', root, re.IGNORECASE):
				foot_list.update(iterate_files(files, 'S1B', directory2, root))

	return (foot_list)


# Call SentinelSat API for querying satellite data
def sat_query_job(footprint, api, satnum, tile_num):

	footprint_rd = geojson_to_wkt(read_geojson(footprint))

	raw_query = ''
	if df.file_name is not None:
	    raw_query = raw_query + 'filename:%s AND ' % df.file_name
	if df.product_type is not None:
	    raw_query = raw_query + 'producttype:%s AND ' % df.product_type
	if df.platform_name is not None:
	    raw_query = raw_query + 'platformname:%s AND ' % df.platform_name
	# if df.orbit_direction is not None:
	#     raw_query = raw_query + 'orbitdirection:%s AND ' % df.orbit_direction
	if df.polarisation_mode is not None:
	    raw_query = raw_query + 'polarisationmode:%s AND ' % df.polarisation_mode
	if df.cloud_cover_percentage is not None:
	    raw_query = raw_query + 'cloudcoverpercentage:%s AND ' % df.cloud_cover_percentage
	if df.sensor_operational_mode is not None:
	    raw_query = raw_query + 'sensoroperationalmode:%s AND ' % df.sensor_operational_mode

    
	if satnum == 'S1A':
		raw_query = raw_query + 'filename:S1A* AND '
		raw_query = raw_query + 'orbitdirection:Descending AND ' 
	elif satnum == 'S1B':
		raw_query = raw_query + 'filename:S1B* AND '
		raw_query = raw_query + 'orbitdirection:Ascending AND '	 
	raw_query = raw_query[:-5]

	# search by polygon, time, and SciHub query keywords
	products = api.query(footprint_rd, date = (df.start_date, df.end_date), raw = raw_query)

	# print results from the search
	if df.printProducts:
	    print "%d products found for " % len(products) + tile_num
	    for product in products:
	        print(products[product]['filename'])

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


	if df.writetoCSV:
		products_df = api.to_dataframe(products)

		now_time = datetime.datetime.now()

		products_df.to_csv(str(now_time)+'.csv')
		print(str(now_time)+'.csv created at ' + output_dir)

def check_files_job(products, output_dir, api):

	pid = []
	error_list = {}
	has_error = False
	for product in products:
		pid.append(product)

	
	if pid and df.checkFiles: 
		error_list = api.check_files(ids = pid, directory = output_dir)
		if error_list:
			has_error = True
			print("error found on " + output_dir)
			with open('error_list.csv', 'wb') as f:
				w = csv.DictWriter(f, error_list.keys())
				w.writeheader()
				w.writerow(error_list)
			
		else:
			# make 'already downloaded files list'
			has_error = False

	return has_error

	
def sql_write_and_rename_job(products, output_dir, foot_list, api, tile_num):

	if df.writeToDB:

		for filename in os.listdir(output_dir):

			if filename.endswith('.zip') and len(filename) > 20 :

				file_list = []
				file_list.append(os.path.join(output_dir,filename))
				check_dict  = api.check_files(paths = file_list)
				#print(os.path.join(output_dir,filename))
				if check_dict:
					print('check_dict: ' + str(check_dict))
					print('Error on ' + filename)

				else:

					conn =sqlite3.connect('sentinelsatDB.db')
					c = conn.cursor()
					c.execute('''CREATE TABLE IF NOT EXISTS scenes (pid text, filename text, date text, satnum text, areacode text, output_dir text)''')
					filename_SAFE = filename.replace('zip','SAFE')
					t = (filename_SAFE, )
					c.execute('SELECT * FROM scenes where filename = ?', t)
					
					if c.fetchone() is None:

						for product in products:
							if products[product]['filename'] == filename_SAFE: 
								pid = product
								file_title = products[product]['filename']
								date = str(products[product]['beginposition']).split()[0]
								satnum = foot_list[1]
								areacode = foot_list[2]
								output_dir = foot_list[0]
								database_write = (pid, file_title, date, satnum, areacode, output_dir)
								c.execute('INSERT INTO scenes VALUES (?,?,?,?,?,?)', database_write)
								print(filename + ' added to inventory.')

						# Rename file
						if df.renameFiles: 

							path = os.path.join(output_dir, filename)
				    		file_param = filename.split('_')
				    		sense_date = file_param[5]
				    		new_filename = sense_date[0:8] + '_' + tile_num + '.zip'
				    		print('Renaming ' + filename + ' to ' + new_filename)
				    		target = os.path.join(output_dir, sense_date[0:8] + '_' + tile_num + '.zip')
				    		os.rename(path, target)

					conn.commit()
					conn.close()


					

	# if df.writeToDB:
	# 	conn =sqlite3.connect('sentinelsatDB.db')
	# 	c = conn.cursor()
	# 	c.execute('''CREATE TABLE IF NOT EXISTS scenes (pid text, filename text, date text, satnum text, areacode text, output_dir text)''')
	# 	for product in products:
	# 		t = (product, )
	# 		c. execute('SELECT * FROM scenes where pid = ?', t)
	# 		temp = c.fetchone()

	# 		if temp is None:
	# 			pid = product
	# 			filename = products[product]['filename']
	# 			date = str(products[product]['beginposition']).split()[0]
	# 			satnum = foot_list[1]
	# 			areacode = foot_list[2]
	# 			output_dir = foot_list[0]
	# 			database_write = (pid, filename, date, satnum, areacode, output_dir)
	# 			c.execute('INSERT INTO scenes VALUES (?,?,?,?,?,?)', database_write)
	# 			print(filename + ' added to inventory.')

				
	# 	conn.commit()
	# 	conn.close()


def sql_read(products):

	products_new = {}
	if df.readDB:
		conn = sqlite3.connect('sentinelsatDB.db')
		c = conn.cursor()
		c.execute('''CREATE TABLE IF NOT EXISTS scenes (pid text, filename text, date text, satnum text, areacode text, output_dir text)''')

		products_new = products
		#print('products_new: ' + str(products_new))
		#print('hello')
		for product in products:
				
			c.execute('SELECT * FROM scenes WHERE pid=?', (product, ))
			temp = c.fetchall()
			if temp:

				#print('temp: ' + str(temp))

				print(products[product]['filename'] + ' is already in the inventory.')
				products_new.pop(product)
				#print(products_new)


		conn.close()

	return products_new



def rename_job(output_dir, directory, tile_num):

	if df.renameFiles:

		for filename in os.listdir(output_dir):

			if filename.endswith('.zip') and len(filename) > 20:

			    ###     parse filename with sensing date    ###

			    path = os.path.join(output_dir, filename)
			    file_param = filename.split('_')
			    sense_date = file_param[5]
			    new_filename = sense_date[0:8] + '_' + tile_num + '.zip'
			    print('Renaming ' + filename + ' to ' + new_filename)
			    target = os.path.join(output_dir, sense_date[0:8] + '_' + tile_num + '.zip')
			    os.rename(path, target)



	
