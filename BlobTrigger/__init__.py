
import logging
import azure.functions as func
import os, io
import pandas as pd

import json    

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
from azure.storage.blob import BlockBlobService

import csv




config = {
    'ENDPOINT': os.environ['ENDPOINT'],
    'PRIMARYKEY': os.environ['PRIMARYKEY'],
    'DBLink': os.environ['DBLink']
}

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    file = myblob.read()
    logging.info (type(file))

    csvf = io.BytesIO(file)    
    logging.info (type(csvf))
    #logging.info (csvf.getvalue())
    csvf = csvf.getvalue().decode('UTF-8')
    
    #logging.info (type(csvf))
    #logging.info (csvf)
    sniffer = csv.Sniffer()
    cnt = sniffer.sniff(csvf) 
    logging.info (cnt.delimiter) 
    df = pd.read_csv(io.BytesIO(file), sep=cnt.delimiter, dtype=str)
    if df.get("ttl") is not None:
       df["ttl"] = pd.to_numeric(df["ttl"])
    logging.info (df)
    results = []
    results = json.loads(df.to_json(orient='records'))
    logging.info (len(results))
    out = []

    client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'], auth={'masterKey': config['PRIMARYKEY']})
    
    # Upload the created file, use local_file_name for the blob name
    #block_blob_service = BlockBlobService(account_name='accountname', account_key='accountkey') 

    #block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)
    
    for item in results:

        logging.info("Import")
        item['id'] = item['CONTRACT_ID']
        
        item = json.dumps(item).replace('null', '""')
        item = json.loads(item)
        logging.info(json.dumps(item,indent=2))
        try:
            logging.info ("Try to create the data....")
            client.CreateItem(config['DBLink'], item)
            logging.info ("Item was created in Cosmos")
            item['Status'] = 'Create'
            out.append(item)
        except errors.HTTPFailure as e:
            #logging.info(e.status_code)
            #logging.info(e._http_error_message) 
            if e.status_code == 409:
                logging.info ("We need to update this id")
                query = {'query': 'SELECT * FROM c where c.id="%s"' % item['id']}
                options = {}
                    
                docs = client.QueryItems(config['DBLink'], query, options)
                doc = list(docs)[0]

                # Get the document link from attribute `_self`
                doc_link = doc['_self']                    
                client.ReplaceItem(doc_link, item)
                item['Status']  ='Update'
                out.append(item)
            else:
                item['Status']  ='Error'
                out.append(item)

    #logging.info (out)

    out = json.dumps(out)

    df=pd.read_json(out)
    
    df.to_csv('results.csv')
    

        # Upload the created file, use local_file_name for the blob name
    block_blob_service = BlockBlobService(account_name=os.environ['account_name'], account_key=os.environ['account_key']) 

    block_blob_service.create_blob_from_path('transferin',  'results.csv', 'results.csv')
    #create_blob_from_path('transferin', output, '/results.csv')
    

    
