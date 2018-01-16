from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
import os 
import schedule
import time, datetime
import sys
#import pandas as pd


# connect to the API
api = SentinelAPI('jldifuntorum', 'copernicus', 'https://scihub.copernicus.eu/dhus')

# download single scene by known product id
#api.download('1f967b94-a263-4bcc-b3bd-395182c21a87')

directory = '/home/kit.difuntorum/GeoJSON_files/'
directory2 = '/home/kit.difuntorum/Sentinel_footprints/'

def download_job(directory, directory2, api):

    for filename in os.listdir(directory):
        if filename.endswith(".geojson"):
            
            filename2=os.path.join(directory, filename)
            print(filename2)
            os.chdir(directory2 + filename)
            footprint =geojson_to_wkt(read_geojson(filename2))
            products = api.query(footprint,
                        date=("NOW-1HOUR","NOW"),
                         platformname='Sentinel-2')

            # for product in products:
            #     #print(product)

            # for columns in products:
            #     print(columns)
            #     for rows in products[columns]:
            #         print (rows,':',products[columns][rows])

            # #odata_prod = api.get_product_odata(product)
            products_df = api.to_dataframe(products)
            # #products_df_sorted = products_df.sort_values(['cloudcoverpercentage', 'ingestiondate'], ascending=[True, True])
            # #products_df_sorted = products_df_sorted.head(5)

            # products_df_sorted = products_df.sort_values(['cloudcoverpercentage', 'ingestiondate'], ascending=[True, True])
            # products_df_sorted = products_df_sorted.head(2)

            now_time = datetime.datetime.now()
            products_df.to_csv("testy.csv")
            new_csv_name = str(now_time) + '.csv'
            os.rename('testy.csv', new_csv_name)

            api.download_all(products)
            # complete_name = os.path.join(directory2+filename, "testy.txt")
            # file1 = open(complete_name, "w")
            # toFile = str(products)
            # file1.write(toFile)
            # file1.close()
            
            
            print('Files last updated on ' + str(now_time))
            #continue
        
        # else:
        #     print('No GeoJSON files are present on the folder')
        #     #sys.exit(1)


download_job(directory, directory2, api)
#schedule.every(3).seconds.do(download_job,directory, directory2, api)

# schedule.every(1).minutes.do(job)
schedule.every(1).hour.do(download_job, directory, directory2, api)
# schedule.every().day.at("10:30").do(download_job)
# schedule.every().monday.do(download_job)
# schedule.every().wednesday.at("13:15").do(download_job)

while True:
    schedule.run_pending()
    #time.sleep(1)

# search by polygon, time, and SciHub query keywords
##footprint = geojson_to_wkt(read_geojson('map.geojson'))
##products = api.query(footprint,
##                     date=('20151219', date(2015, 12, 29)),
##                     platformname='Sentinel-2')

