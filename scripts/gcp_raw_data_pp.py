'''Get raw data from data lake, preprocess it and save into data warehouse.

This script gets the raw dataset with all rent offers in the predefinated cities
in Germany, separate the meaningful information, preprocess it and organizes in 
separated columns columns.

Save the new csv file as "all_offers_infos_pp.csv" into "de_rent_bkt"
'''
    
# imports
import re
import os
import logging
import numpy as np
import pandas as pd
from google.cloud import storage

# set log folder, files and object configs
if not os.path.exists('Logs'):
    os.makedirs('Logs')
    
logging.basicConfig(
    filename='Logs/offers_infos_cleaner.txt',
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m_%d %H:%M:%S',
    level=logging.DEBUG
)

logger = logging.getLogger('offers_infos_cleaner')


def offers_infos_preprocess(df_raw, save=True):
    '''Clean and separate meaningful infos
    
    Parameter:
    ----------
        df_raw: Dataframe to be cleaned
        save: save the dataframe into GCP Storage data warehouse
            default: True        
    Return:
    -------
        Returns a new dataframe with the meaningful informations separated by columns
        and cleaned.    
    '''
    # Separate into latitude (lat) and longitude (lng)
    df_raw['lat'] = df_raw['lat_lng'].apply(lambda x: re.findall('\d+.\d+', x)[0])
    df_raw['lng'] = df_raw['lat_lng'].apply(lambda x: re.findall('\d+.\d+', x)[1])

    # drop original lat_lng column
    df_raw.drop(columns='lat_lng', inplace=True)

    df_list = []

    for x in range(len(df_raw)):
        infos_dict = {}  
        
        # get infos from df_raw
        infos_dict['offer_id'] = df_raw['offer_id'][x]
        infos_dict['extraction_date'] = df_raw['extraction_date'][x]
        infos_dict['lat'] = df_raw['lat'][x]
        infos_dict['lng'] = df_raw['lng'][x]
        infos_dict['city'] = df_raw['city'][x]

        # preprocess the infos cell
        b = df_raw['script_infos'][x].replace('\\', '')
        b = b.replace('{', '').replace('}', '')[4:]
        b = b[:-1]
        b = b.split(',')
        
        # get all meaningful infos and return it cleane and
        # separated by columns.
        for i in b:
            # offer area
            if 'area' in i:
                try:
                    i = i.replace('"', '').replace("'", "").replace('area:', '').replace(' ', '')
                    infos_dict['area_m2'] = float(i)
                except:
                    infos_dict['area_m2'] = np.nan
                    logger.debug(f"Offer {i} has no information about area.")
            # if the offer is furnished or not
            if 'mobex' in i:
                if 'true' in i:
                    infos_dict['furnished'] = 1
                elif 'false' in i:
                    infos_dict['furnished'] = 0
                else:
                    infos_dict['furnished'] = np.nan
                    logger.debug(f'Offer {i} has no information about furniture.')
            #else:
            #    infos_dict['furnished'] = np.nan
            #    logger.debug(f'Offer {i} has no information about furniture.')
            # the offer zip code 
            if 'zip' in i:
                try:
                    infos_dict['zip_code'] = int(re.findall('\d+', i)[0])
                except:
                    infos_dict['zip_code'] = np.nan
                    logger.debug(f'Offer {i} has no information about zip_code.')
            # offer category
            if 'objectcat' in i:
                try:
                    infos_dict['main_category'] = re.findall('\:"\w+"', b[3])[0][1:].replace('"', '')
                except:
                    infos_dict['main_category'] = np.nan
                    logger.debug(f'Offer {i} has no information about main category.')
            # number of rooms
            if 'rooms' in i:
                try:
                    infos_dict['rooms'] = float(re.findall('\d+', i)[0])
                except:
                    infos_dict['rooms'] = np.nan
                    logger.debug(f'Offer {i} has no information about number of rooms.')
            # build yuear of construction
            if 'buildyear' in i:
                try:
                    infos_dict['build_year'] = int(re.findall('\d+', i)[0])
                except:
                    infos_dict['build_year'] = np.nan
                    logger.debug(f'Offer {i} has no information about build construction year.')
            # state
            if 'fed' in i:
                try:
                    infos_dict['state'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['state'] = np.nan
                    logger.debug(f'Offer {i} has no information about state.')
            # city
            #if 'city' in i:
            #    try:
            #        infos_dict['city'] = i.split(':')[1].replace('"', '')
            #    except:
            #        infos_dict['city'] = np.nan
            #        logger.debug(f'Offer {i} has no information about city.')
            # offer sub-category
            if 'obcat' in i:
                try:
                    infos_dict['sub_category'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['sub_category'] = np.nan
                    logger.debug(f'Offer {i} has no information about sub-category.')
            # if the offer has or not a "balcon"- balcony
            if 'balcn' in i:
                if 'true' in i:
                    infos_dict['balcony'] = 1
                elif 'false' in i:
                    infos_dict['balcony'] = 0
                else:
                    infos_dict['balcony'] = np.nan
                    logger.debug(f'Offer {i} has no information about balcony.')
            #else:
            #    infos_dict['balcony'] = np.nan
            #    logger.debug(f'Offer {i} has no information about balcony.')
            # heat type
            if 'heatr' in i:
                try:
                    infos_dict['heat_type'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['heat_type'] = np.nan
                    logger.debug(f'Offer {i} has no information about heat type.')
            # offer title
            if 'title' in i:
                try:
                    infos_dict['offer_title'] = i.split(':')[1].replace('"', '')
                except:
                    infos_dict['offer_title'] = np.nan
                    logger.debug(f'Offer {i} has no information about offer title.')
            # if the offer has already a kitchen
            if 'kitch' in i:
                if 'true' in i:
                    infos_dict['kitchen'] = 1
                elif 'false' in i:
                    infos_dict['kitchen'] = 0
                else:
                    infos_dict['kitchen'] = np.nan
                    logger.debug(f'Offer {i} has no information about kitchen.')
            #else:
            #    infos_dict['kitchen'] = np.nan
            #    logger.debug(f'Offer {i} has no information about kitchen.')
            if 'gardn' in i:
                if 'true' in i:
                    infos_dict['garden'] = 1
                elif 'false' in i:
                    infos_dict['garden'] = 0
                else:
                    infos_dict['garden'] = np.nan
                    logger.debug(f'Offer {i} has no information about garden.')
            #else:
            #    infos_dict['garden'] = np.nan
            #    logger.debug(f'Offer {i} has no information about garden.')
            # offer rent price
            if 'price' in i:
                try:
                    infos_dict['rent_price'] = float(re.findall('\d+', i)[0])
                except:
                    infos_dict['rent_price'] = np.nan
                    logger.debug(f'Offer {i} has no information rent price.')
                    
        # append the infos about the offer           
        df_list.append(infos_dict)
        #logger.info(f'Offer no. {i} cleaned.')
        
    # create a new cleaned dataframe
    df_pp = pd.DataFrame(df_list)
    
    if save:
        if not os.path.exists('../data'):
            os.makedirs('../data')
        output_dir = '../data/'
        filename = f'all_offers_infos_pp.csv'
        df_pp.to_csv(os.path.join(output_dir, filename), index=False)
        

    return None

def upload_blob():
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    bucket_name = "de_rent_bkt"
    # The path to your file to upload
    source_file_name = "../data/all_offers_infos_pp.csv"
    # The ID of your GCS object
    destination_blob_name = "de_rent_data/all_offers_infos_pp.csv"
    
    storage_client = storage.Client.from_service_account_json(
        '/Users/felipedemenechvasconcelos/keys/scenic-edition-310913-26647dbaf7a5.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def main():
    
    df_raw = pd.read_csv('../data/all_offers_infos_raw.csv')
    df_cleaned = offers_infos_preprocess(df_raw)
    upload_blob()

if __name__=='__main__':
    main()