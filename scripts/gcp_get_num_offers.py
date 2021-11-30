import re
import os
import requests
import pandas as pd
from time import sleep
from random import randint
from datetime import datetime
from bs4 import BeautifulSoup
from google.cloud import storage


def get_offers_qtt(save=True):
    '''
    Get offers quantity by city and store it in a postgres DB.
    '''
    
    cities_dict = {
        'Dusseldorf': 100207,
        'Berlin': 87372,
        'Essen': 102157,
        'Munchen': 121673,
        'Koln': 113144,
        'Stuttgart': 143262,
        'Dresden': 100051,
        'Hannover': 109489,
        'Dortmund': 99990,
        'Frankfurt am Main': 105043,
        'Hamburg': 109447
    }
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now2 = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    # get all offers quantity - haus und wohnung

    cities_offers = []

    for city, code in cities_dict.items():
        
        #logging.info(f'Getting offers in {city}...')
        print(f"Getting offers in {city}...")

        total_offers = 0

        for i in range(1,3):
            url = f'https://www.immonet.de/immobiliensuche/sel.do?&sortby=0&suchart=1&objecttype=1&marketingtype=2&parentcat={i}&city={code}'
            #url = f'https://www.immonet.de/immobiliensuche/sel.do?parentcat={i}&objecttype=1&pageoffset=378&listsize=27&suchart=1&sortby=0&city={code}&marketingtype=2&page=1'
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")

            # Find number of rent offers.
            a = soup.select('ul', class_='tbl margin-auto margin-top-0 margin-bottom-0 padding-0')
            
            for i in a:
                if 'Alle Orte' in i.text:
                    c = i.text
                    total_offers += int(re.findall('\d+', c)[0])
            #total_offers += int(re.search('\d+', a).group())

        cities_offers.append({'extraction_datetime': now, 'city': city, 'city_code': code, 'offers': total_offers})

    # offers_by_page = len(soup.findAll('div', class_="col-xs-12 place-over-understitial sel-bg-gray-lighter"))    
    df_offers = pd.DataFrame(cities_offers)
    #df_offers.to_csv(f'temp_data/total_offers_by_city_temp_file.csv', index=False)
    if save:
        #if not os.path.exists('../data'):
            #os.makedirs('../data')
        df_offers.to_csv('../data/offers_qtt_by_city.csv', index=False)

    return None


def upload_blob():
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    bucket_name = "de_rent_bkt"
    # The path to your file to upload
    source_file_name = "../data/offers_qtt_by_city.csv"
    # The ID of your GCS object
    destination_blob_name = "de_rent_data/de_rent_data"
    
    storage_client = storage.Client.from_service_account_json(
        '/Users/felipedemenechvasconcelos/keys/scenic-edition-310913-26647dbaf7a5.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    
def main():
    get_offers_qtt()
    upload_blob()
    
if __name__ == '__main__':
    main()
