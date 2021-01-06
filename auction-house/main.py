import requests, logging, os, json
from google.cloud import bigquery
from datetime import datetime

bq_project = os.environ["PROJECT"]
bq_dataset = os.environ["DATASET"]
bq_table = os.environ["TABLE"]
bq_table_id = bq_project + "." + bq_dataset + "." + bq_table
wow_client_id = os.environ["WOW_CLIENT_ID"]
wow_client_secret = os.environ["WOW_CLIENT_SECRET"]
realm_id = os.environ["REALM_ID"]

def write_auction_data_to_bq(request):
    
    try:
        logging.info('Attempting to retrieve auth token from battle.net with client ID {}'.format(wow_client_id))
        response = create_access_token(wow_client_id, wow_client_secret)
        token = response['access_token']
    except:
        logging.error('Failed to retrieve battle.net token')
        raise
    
    try: 
        logging.info('Attempting to retrieve auction data from realm {} '.format(realm_id) )
        auction_data = get_auction_data(token, realm_id)
    except:
        logging.error('Failed to retrieve auction data')
        raise
    
    try: 
        logging.info('Attempting to write auction date to file')
        file_path = auction_data_to_file(auction_data)
    except:
        logging.error('Failed to write auction data to file')
        raise
    
    try:
        logging.info('Attempting to write rows to table {} '.format(bq_table_id) )
        bq_insert_rows(file_path)
        logging.info('Rows have been successfully written to table {} '.format(bq_table_id))
    except:
        logging.error('Failed to write to BQ table')
        raise
    
# Create a new Access Token
def create_access_token(client_id, client_secret, region = 'us'):
    data = { 'grant_type': 'client_credentials' }
    response = requests.post('https://%s.battle.net/oauth/token' % region, data=data, auth=(client_id, client_secret))
    return response.json()

def get_auction_data(token, realm_id):
    search = "https://us.api.blizzard.com/data/wow/connected-realm/" + realm_id + "/auctions?namespace=dynamic-us&locale=en_US&access_token=" + token
    response = requests.get(search)
    return response.json()["auctions"]

def auction_data_to_file(result):
    
    dest_file = '/tmp/tmp.json'
    
    with open(dest_file, 'w') as f:
        
        for row in result:
            if 'bonus_lists' not in row['item']:
                row['item']['bonus_lists'] = []
        
            if 'modifiers'not in row['item']:
                row['item']['modifiers'] = []
            
            row['ingested_at'] = datetime.utcnow().isoformat()
            
            json.dump(row, f)
            f.write('\n')
    
    logging.info('Data has been written to file {}'.format(dest_file))
    
    return dest_file

def bq_insert_rows(source_file_name):
    '''
        Inserts the final message to BQ.
    '''

    bq_client = bigquery.Client(bq_project)

    job_config = bigquery.LoadJobConfig(
    source_format='NEWLINE_DELIMITED_JSON'
    )

    with open(source_file_name, 'rb') as source_file:
        job = bq_client.load_table_from_file(
            source_file, bq_table_id, job_config=job_config
        )

    job.result()