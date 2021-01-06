import requests, logging, os, json
from google.cloud import bigquery
from datetime import datetime

bq_project = 'world-of-warcraft-300101'
bq_dataset = 'wow'
bq_table = 'auction_house'
bq_table_id = bq_project + "." + bq_dataset + "." + bq_table
wow_client_id = 'f75cb20bac744a74bcf1f579e344f58e'
wow_client_secret = 'GtazA4PkC274dFv76etrhxUIvTlaYbn9'
realm_id = '77'

bq_client = bigquery.Client(bq_project)

table = bq_client.get_table(bq_table_id)
    
# Create a new Access Token
def create_access_token(client_id, client_secret, region = 'us'):
    data = { 'grant_type': 'client_credentials' }
    response = requests.post('https://%s.battle.net/oauth/token' % region, data=data, auth=(client_id, client_secret))
    return response.json()

def get_auction_data(token, realm_id):
    search = "https://us.api.blizzard.com/data/wow/connected-realm/" + realm_id + "/auctions?namespace=dynamic-us&locale=en_US&access_token=" + token
    response = requests.get(search)
    return response.json()["auctions"]

def bq_insert_rows(source_file_name):
    
    job_config = bigquery.LoadJobConfig(
    source_format='NEWLINE_DELIMITED_JSON'
    )

    with open(source_file_name, 'rb') as source_file:
        job = bq_client.load_table_from_file(
            source_file, bq_table_id, job_config=job_config
        )

    job.result()
    
response = create_access_token(wow_client_id, wow_client_secret)
token = response['access_token']

auction_data = get_auction_data(token, realm_id)

logging.info("inserting rows to table %s", bq_table_id)

# for item in auction_data:
    
#     if 'bonus_lists' not in item['item']:
#         item['item']['bonus_lists'] = []
    
#     if 'modifiers'not in item['item']:
#         item['item']['modifiers'] = []
    
#     bq_insert_rows(item)

#     dest_file = '/tmp/tmp.csv'

def auction_data_to_bq_rows(result):
    
    dest_file = 'tmp.json'
    
    with open(dest_file, 'w') as f:
        
        for row in result:
            if 'bonus_lists' not in row['item']:
                row['item']['bonus_lists'] = []
        
            if 'modifiers'not in row['item']:
                row['item']['modifiers'] = []
            
            row['ingested_at'] = datetime.utcnow().isoformat()
            
            json.dump(row, f)
            f.write('\n')
    
    return dest_file

file_path = auction_data_to_bq_rows(auction_data)
bq_insert_rows(file_path)