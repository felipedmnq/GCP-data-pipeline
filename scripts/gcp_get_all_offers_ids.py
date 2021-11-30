'''Get the ids from all rent offers in all determinated cities.

This script gets all the ids from all rent offers in all the predeterminated cities
and save it in a database table named "all_offer_ids" to further use.

The previous table is dropped and a new table is created each time that it runs.
'''

import os
import re
import sys
import math
import requests
import psycopg2
import logging
import numpy as np
import pandas as pd

from time import sleep
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2.extras as extras
from google.cloud import storage
from sqlalchemy import create_engine

# Create log folder if not exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')
    
logging.basicConfig(
    filename='Logs/get_offer_ids.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('get_offer_ids')


def get_offer_ids(df, save=True):
    '''Get all offer ids for each offer in each city
    
    Parameters:
    -----------
        df: a dataframe with the number of offers in each city.
        
        save: default=False
            save the returned dataframe locally or not.
            
    Return:
    -------
        Return a dataframe with all offer ids for each city with the type of the offer (wohnung/haus).
    '''    
    
    ids_list = []
    offers_by_page = 26

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for c in range(len(df)):
        city = df.loc[c]['city']
        city_code = df.loc[c]['city_code']
        offers = df.loc[c]['offers']
        
        print(f'Getting offer ids for {city}...')
        logger.info(f'Getting offer ids for {city}...')

        # get all offers ids for haus und wohnung in each city


        # Get the number of pages to scrape - rounded to down
        number_of_pages = math.floor(offers / offers_by_page)

        # wohnung/haus code
        l_opt = [1, 2]

        for opt in l_opt:
            for page in range(number_of_pages):
                url = f"https://www.immonet.de/immobiliensuche/sel.do?parentcat={opt}&objecttype=1&pageoffset=1&listsize=26&suchart=1&sortby=0&city={city_code}&marketingtype=2&page={page}"
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                page = requests.get(url, headers=headers)
                soup = BeautifulSoup(page.text, "html.parser")

                offers_list_1page = soup.findAll('div', class_="col-xs-12 place-over-understitial sel-bg-gray-lighter")
                
                for i in range(len(offers_list_1page)):
                    try:
                        if opt == 1:
                            ids_list.append({'extraction_datetime': now, 'offer_id': offers_list_1page[i].find('a')['data-object-id'], 'city': city, 'city_code': city_code, 'type': 'wohnung'})
                        if opt == 2:
                            ids_list.append({'extraction_datetime': now, 'offer_id': offers_list_1page[i].find('a')['data-object-id'], 'city': city, 'city_code': city_code, 'type': 'haus'})
                    except:
                        logger.error(f'Error - id:{i}')
                        pass
                sleep(randint(1, 2))         
        sleep(randint(1, 5))          

    # Create a dataframe with the infos
    df_ids = pd.DataFrame(ids_list)
    df_ids.drop_duplicates(subset='offer_id', inplace=True)

    # save as csv file
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        output_dir = '../data/'
        #now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        filename = f'all_offers_ids.csv'
        df_ids.to_csv(os.path.join(output_dir, filename), index=False)
    
    return None

def upload_blob():
    """Uploads a file to the bucket."""
    # GCS bucket ID
    bucket_name = "de_rent_bkt"
    # Path file to upload
    source_file_name = "../data/all_offers_ids.csv"
    # GCS object ID
    destination_blob_name = "de_rent_data/all_offers_ids.csv"
    
    storage_client = storage.Client.from_service_account_json(
        '/Users/felipedemenechvasconcelos/keys/scenic-edition-310913-26647dbaf7a5.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    
def main():
    # get data from local storage
    df = pd.read_csv('../data/offers_qtt_by_city.csv')
    # get all offer ids and save localy as a csv file
    get_offer_ids(df)
    # get the csv file localy and load into cloud storage
    upload_blob()
    
if __name__=='__main__':
    main()