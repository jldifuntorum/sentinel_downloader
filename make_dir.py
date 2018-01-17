import os

directory = '/Users/kitdifuntorum/Documents/GeoJSON files/s1b_geojson'
directory2 = '/Users/kitdifuntorum/Documents/Sentinel Footprints/'

os.chdir(directory2)

for filename in os.listdir(directory):

	if not os.path.exists(filename):
		os.makedirs(filename)
