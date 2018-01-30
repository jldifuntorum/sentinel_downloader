# login credentials
username = 'jldifuntorum'
password = 'copernicus'
url = ''

# directories
areacode = ''
geojson_dir = r''
output_dir = r''

directory = '/Users/kitdifuntorum/Documents/GeoJSON files/'
directory2 = '/Users/kitdifuntorum/Documents/Sentinel Footprints/'

#	This folder gets purged monthly
temp_dir = '/Users/kitdifuntorum/Documents/Temp'

# query keywords
start_date = 'NOW-3DAY'
end_date = 'NOW'
file_name = None
product_type = 'SLC'
platform_name = None
orbit_direction = None
polarisation_mode = None
cloud_cover_percentage = None
sensor_operational_mode = None
sat_limiter = 'both'
max_download_retry = 5

# post-search modes
printProducts = True
writeToDB = True
downloadProducts = False
getGeoJSON = False
writetoCSV = False
checkFiles = True	#Note: Downloader will always produce an error if checkFiles == True and downloadProducts == False
renameFiles = True
readDB = True


