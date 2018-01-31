from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import os 
import schedule
import time, datetime
import pandas as pd
import glob 
import re
import sqlite3
import def_values_for_hpc as df
import csv
from collections import defaultdict
import logging

#Function Declarations

logging.basicConfig(filename='error_sentinel.log',level=logging.ERROR)

def sql_connect():
	conn = sqlite3.connect('sentinelsatDB.db')
	c = conn.cursor()

	return c, conn

def sql_close(conn):

	conn.commit()
	conn.close()

def sql_update(c, conn, stat, product):

	c.execute('''UPDATE scenes SET status = ? WHERE pid = ? ''', (stat, product))
	conn.commit()


def sql_new_entry(c, products, product, status, output_dir, foot_list):

	pid = product
	file_title = products[product]['filename']
	date = str(products[product]['beginposition']).split()[0]
	satnum = foot_list[1]
	areacode = foot_list[2]
	output_dir = foot_list[0]
	database_write = (pid, file_title, date, satnum, areacode, output_dir, status)
	c.execute('INSERT INTO scenes VALUES (?,?,?,?,?,?,?)', database_write)
	print(file_title + ' added to database inventory.')

def database_init():

	c, conn = sql_connect()
	c.execute('SELECT * FROM scenes WHERE status=?', ('DOWNLOADING', ))
	print 'Changing status of unfinished downloads to QUEUED'
	for downloads in c.fetchall():
		#c.execute('''DELETE FROM scenes WHERE pid = ? ''', (downloads[0],))
		sql_update(c, conn, 'QUEUED', downloads[0])
	sql_close(conn)


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

# Download scenes not on the database
def download_job(products, output_dir, api):

	# download all results from the search
	if df.downloadProducts:
		c, conn = sql_connect()

		for product in products:

			#IF STAT == QUEUED:
			if df.writeToDB:
				sql_update(c, conn, 'DOWNLOADING', product)
				print('Database updated: Downloading ' + products[product]['filename'])

			api.download(product, directory_path = output_dir)

		sql_close(conn)

	if df.writetoCSV:
		products_df = api.to_dataframe(products)

		now_time = datetime.datetime.now()

		products_df.to_csv(str(now_time)+'.csv')
		print(str(now_time)+'.csv created at ' + output_dir)


# Check for file integrity
def check_files_job(products, output_dir, api):

	pid = []
	error_list = {}
	for product in products:
		pid.append(product)

	
	if pid and df.checkFiles: 
		error_list = api.check_files(ids = pid, directory = output_dir)
		#print ('error_list: ' + str(error_list))
		if error_list:
			for errors in error_list:
				print('Error on file ' + str(error_list[errors][0]['title']) + ' located at ' + output_dir)
				logging.error('Error on file ' + str(error_list[errors][0]['title']) + ' located at ' + output_dir + '\n')
				logging.error('Date checked: ' + str(datetime.datetime.now()) + '\n')
			
			# Redownload file in case of error
			if df.downloadProducts:
				print 'Retrying download of ' + str(error_list[errors][0]['title'])
				api.download(error_list[errors][0]['id'], output_dir)

			error_list = api.check_files(ids = pid, directory = output_dir)	

	return error_list


# Write completed scene to database and rename the file
def sql_write_and_rename_job(products, output_dir, foot_list, api, tile_num):

	check_dict = defaultdict(list)
	if df.writeToDB:

		for filename in os.listdir(output_dir):

			if filename.endswith('.zip') and len(filename) > 20 :

				c, conn = sql_connect()
				filename_SAFE = filename.replace('zip', 'SAFE')
				t = (filename_SAFE, )
				c.execute('SELECT * FROM scenes where filename = ?', t)
				file_comp = c.fetchone()

				if file_comp is None:

						#do something
						print('Error on file ' + filename_SAFE + ' located at ' + output_dir + '. File is not on database inventory')
						logging.error('Error on file ' + filename_SAFE + ' located at ' + output_dir + '. File is not on database inventory' + '\n')
						logging.error('Date checked: ' + str(datetime.datetime.now()) + '\n')

				else:

						if file_comp[6] == 'COMPLETED':

							continue
						else:

							file_list = []
							file_list.append(os.path.join(output_dir,filename))
							check_dict  = api.check_files(paths = file_list)
							
							if check_dict:
								if df.checkFiles:
									for file_zip in check_dict:
										print 'ERROR: ' + file_zip + ' does not match ' + str(check_dict[file_zip][0]['title']) + ' on Copernicus server.'
										logging.error(file_zip + ' does not match ' + str(check_dict[file_zip][0]['title']) + ' on Copernicus server.' + '\n')
										logging.error('Date checked: ' + str(datetime.datetime.now()) + '\n')
					
										if df.downloadProducts:
											print 'Retrying download of ' + file_zip
											api.download(check_dict[file_zip][0]['id'], output_dir)

							else: 

								sql_update(c, conn, 'COMPLETED', file_comp[0])
								print(file_comp[1] + ' download complete.')
								print('File saved at ' + output_dir + '\n')

								if df.renameFiles: 

									path = os.path.join(output_dir, filename)
						    		file_param = filename.split('_')
						    		sense_date = file_param[5]
						    		new_filename = sense_date[0:8] + '_' + tile_num + '.zip'
						    		print('Renaming ' + filename + ' to ' + new_filename)
						    		target = os.path.join(output_dir, sense_date[0:8] + '_' + tile_num + '.zip')
						    		os.rename(path, target)

				sql_close(conn)


				
#  Read the database




def sql_read(products, output_dir, foot_list):

	products_new = {}
	if df.readDB:
		c, conn = sql_connect()
		c.execute('''CREATE TABLE IF NOT EXISTS scenes (pid text, filename text, date text, satnum text, areacode text, output_dir text, status text)''')

		products_new = products
		for product in products:
				
			c.execute('SELECT * FROM scenes WHERE pid=?', (product, ))
			row = c.fetchone()
			if row:
				
				if row[6] == 'QUEUED':

					continue
				else:

					if row[6] == 'DOWNLOADING':
						print(row[1] + ' is already downloading')
					elif row[6] == 'COMPLETED':
						print(row[1] + ' is already downloaded')
					
					products_new.pop(product)

			else:
				if df.writeToDB:
					sql_new_entry(c, products_new, product, 'QUEUED', output_dir, foot_list)

		sql_close(conn)

	return products_new





	
