'''Get all rent offer infos - raw

This script gest all rent offer infos from each city.
It uses the ids from the rent offers and extract all the information 
from the offer and saves it in a row format in database table named
"all_offers_infos".

Each row contains all the rent offer information in a raw format, with 
no preprocessing. 

It gets infos from two sources inside the webpage: "scripts" and "html"
and save each in a different column.

The returned data is saved localy as "all_offers_infos_raw.csv" and 
injested into GCP Storage data lake - bucket: "de_rent_raw".
'''

import os
import re
import math
import requests
#import psycopg2
import logging
import numpy as np
import pandas as pd

from tqdm import tqdm
from time import sleep
#from config import config
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
#import psycopg2.extras as extras
#from sqlalchemy import create_engine
#from get_offers_by_city import connect


# Create log folder if not exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')
    
logging.basicConfig(
    filename='Logs/get_offers_infos.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('get_offers_infos')



# get all infor for all founded offers
def get_offers_infos(df_ids, save=True):
    '''Get all infos from all rent offers
    
    The function saves the result into "data" folder as
    "all_offers_infos_raw.csv"
    
    Params:
    -------
        df_ids: offers ids source.
        save: save the returned dataframe locally or not.
            default: True
    Returns:
    --------
        A dataframe with all offer informations in a row format.
    '''
    
    infos_list = []
    count = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print('Start data extraction...')
    with tqdm(total=df_ids.shape[0]) as pbar:
        for Id in set(df_ids['offer_id']):

            url = f"https://www.immonet.de/angebot/{Id}?drop=sel&related=false&product=standard"
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")

            #panels_infos = soup.findAll('div', class_='row box-50')
            # get all infos about each offer (all infos are mixed)

            # get all infos in almost a json format
            script_infos = soup.select('script', type_='text/javascript')
            
            for i in script_infos:
                if 'targetingParams' in i.text:
                    c = i.text
                    infos1 = re.findall('\{(.*?)\}', c)
                
                # get lat/lng
                if 'initModalMap' in i.text:
                    try:
                        c = i.text.replace('\n', '').replace('\t', '')
                        lat_lng = re.findall('\{lat: \d+.\d+,lng: \d+.\d+}', c)
                    except:
                        lat_lng = np.nan
                        
            # get infos from other source
            try:
                panels_infos = soup.findAll('div', class_='row margin-top-18')
            except:
                panels_infos = np.nan



            infos_list.append({'offer_id': Id,
                               'extraction_date': now,
                               'city': df_ids['city'][count],
                               'city_code': df_ids['city_code'][count],
                               'offer_type': df_ids['type'][count],
                               'lat_lng': lat_lng,
                               'script_infos': infos1,
                               'html_infos': panels_infos})
            sleep(1)
            pbar.update(1)
            #print(count)
            count += 1

    df_infos = pd.DataFrame(infos_list)
    df_infos.drop_duplicates(subset='offer_id', inplace=True)
    logger.info('df_infos created')
    print('Data extraction completed.')
    
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        output_dir = '../data/'
        #now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        filename = f'all_offers_infos_raw.csv'
        df_infos.to_csv(os.path.join(output_dir, filename), index=False)
        
    return df_infos

def upload_blob():
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    bucket_name = "de_rent_raw"
    # The path to your file to upload
    source_file_name = "../data/all_offers_infos_raw.csv"
    # The ID of your GCS object
    destination_blob_name = "de_rent_data/all_offers_infos_raw.csv"
    
    storage_client = storage.Client.from_service_account_json(
        '/Users/felipedemenechvasconcelos/keys/scenic-edition-310913-26647dbaf7a5.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def main():
    df_ids = pd.read_csv('../data/all_offers_ids.csv')
    get_offers_infos(df_ids)
    upload_blob()

if __name__=='__main__':
    main()