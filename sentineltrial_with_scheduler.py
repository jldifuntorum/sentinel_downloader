#John Keithley Difuntorum

from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
import os 
import schedule
import time, datetime
import sys
import pandas as pd
import glob 
import re

# connect to the API
api = SentinelAPI('jldifuntorum', 'copernicus')

# download single scene by known product id
#api.download('1f967b94-a263-4bcc-b3bd-395182c21a87')

directory = '/Users/kitdifuntorum/Documents/GeoJSON files/'
directory2 = '/Users/kitdifuntorum/Documents/Sentinel Footprints/'

# products_init = {}
# products_new = {}
# products_df = {}

#disregard hidden files in iteration
def listdir_nohidden(path):
    return glob.glob(os.path.join(path, '*'))

def download_job(directory, directory2, api, initial):

    for subdir in listdir_nohidden(directory):
        dir_subdir=os.path.join(directory, subdir)
        
        for filename in os.listdir(dir_subdir):
            if filename.endswith(".geojson"):
                
                #print("subdir: " + subdir)
                filename2=os.path.join(dir_subdir, filename)
                
                #print("filename2: " + filename2)

                subdir_min = subdir.replace(directory, '')
                filename_min = filename.replace('.geojson', '')
                
                # print(subdir_min)
                # print(filename_min)

                if re.search('s1a', subdir_min, re.IGNORECASE):
                    scene_dir = os.path.join(directory2 + 'S1A/', filename_min)
                elif re.search('s1b', subdir_min, re.IGNORECASE): 
                    scene_dir = os.path.join(directory2 + 'S1B/', filename_min)


                if not os.path.exists(scene_dir):
                    os.makedirs(scene_dir)
                    print(scene_dir + " folder created.")

                
                #print("scene_dir: " + scene_dir)
                
                os.chdir(scene_dir)


                footprint = geojson_to_wkt(read_geojson(filename2))
                products = api.query(footprint,
                            date=("NOW-1MONTH","NOW"),
                             platformname='Sentinel-2')

               

                # #odata_prod = api.get_product_odata(product)

                products_df = api.to_dataframe(products)
                # if initial == 'TRUE': 
                #     products_init = products_df
                #     products_out  = products_df

                # #products_df_sorted = products_df.sort_values(['cloudcoverpercentage', 'ingestiondate'], ascending=[True, True])
                # #products_df_sorted = products_df_sorted.head(5)

                # products_df_sorted = products_df.sort_values(['cloudcoverpercentage', 'ingestiondate'], ascending=[True, True])
                # products_df_sorted = products_df_sorted.head(2)

                now_time = datetime.datetime.now()

                products_df.to_csv(str(now_time)+'.csv')
                print(str(now_time)+'.csv created at ' + scene_dir)
                api.download('284f63cf-6488-4f22-8ed2-404a104b103d')

                ####    COMPARE PRODUCTS INIT AND PRODUCTS DF      #####

                # df_diff = pd.concat([products_init, products_df])
                # df_diff = df_diff.reset_index(drop=True)
                # df_gpby = df_diff.groupby(list(df_diff.columns))
                # idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
                # df_diff.reindex(idx)

                # df_diff = products_init.merge(products_df, indicator=True, how='outer')

                # df_diff[df_diff['_merge'] == 'right_only']
                # print(df_diff)

                #df_diff = pd.merge(products_init, products_df, indicator=True, how='outer')
                #print(df_diff)
                # both = (df_diff['_merge'] == 'both').sum()
                # #print (both)
                # left_only = df_diff.loc[df_diff['_merge'] == 'left_only', products_init.columns]
                # print ('left_only')
                # print (left_only)
                # right_only = df_diff.loc[df_diff['_merge'] == 'right_only', products_df.columns]
                # print ('right_only')
                # print (right_only)

                ####    Create new products_diff for products_df - products INIt ######
                ####    Write products_diff to out_csv file        #####

                # products_out.append(df_diff)
                # products_out.to_csv('testy.csv')
                #df_diff = {}
                #if initial == 'TRUE': products_diff.to_csv('testy.csv')


                ####    Download products_diff                     #####
                ####    Set products init to products df           #####

                #products_init = products_df
                # new_csv_name = str(now_time) + '.csv'
                # os.rename('testy.csv', new_csv_name)

                #api.download_all(products)
                # complete_name = os.path.join(directory2+filename, "testy.txt")
                # file1 = open(complete_name, "w")
                # toFile = str(products)
                # file1.write(toFile)
                # file1.close()
                
                
                #print('Files last updated on ' + str(now_time))
                #continue
            
            # else:
            #     print('No GeoJSON files are present on the folder')
            #     #sys.exit(1)

        # return products_df


download_job(directory, directory2, api, 'TRUE')
sys.exit(1)

####products_init = products_df
#####       

schedule.every(60).seconds.do(download_job, directory, directory2, api, 'FALSE')


# products_init.to_csv('testy1.csv')
# products_new.to_csv('testy2.csv')

# schedule.every(1).minutes.do(job)
# schedule.every(1).hour.do(download_job, directory, directory2, api, products_init)
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

