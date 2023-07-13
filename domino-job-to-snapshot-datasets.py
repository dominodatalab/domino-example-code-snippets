
import boto3
import requests
import boto3
import os
from datetime import datetime
import json
import time


def get_headers():
    token = requests.get('http://localhost:8899/access-token')
    domino_token = token.text
    
    headers = {
      'Authorization': f'Bearer {domino_token}',
      'Content-Type': 'application/json'
    }
    return headers

def create_snapshot():
    host = os.environ['DOMINO_API_HOST']
    project_id = os.environ['DOMINO_PROJECT_ID']
    url = f"{host}/api/datasetrw/v2/datasets"
    print(url)
    payload = {}
    response = requests.request("GET", url, headers=get_headers(), data=payload, params={'limit':10000})
    my_dataset = None
    for d in response.json()['datasets']:
        #print(d['dataset']['projectId'])
        if d['dataset']['projectId']==project_id:        
            my_dataset = d

    if my_dataset:
        print(my_dataset)
        dataset_id = my_dataset['dataset']['id']

        url = f"{host}/api/datasetrw/v1/datasets/{dataset_id}/snapshots"
        print(url)
        payload = json.dumps({
          "relativeFilePaths": [
                ""
              ]
        })
        response = requests.request("POST", url, headers=get_headers(),data=payload)
        print(response.status_code)
        print(response.json())
        snapshot_id = response.json()['snapshot']['id']
        print(snapshot_id)   
        is_active = False
        ## Wait util snapshot is completed
        while (not is_active):
            url = f"{host}/api/datasetrw/v1/snapshots/{snapshot_id}"
            response = requests.request("GET", url, headers=get_headers())
            print(response.status_code)
            print(response.json())
            is_active = response.json()['snapshot']['status']
            time.sleep(1)
    
# Use IRSA to read from S3 bucket and write to the Dataset
def write_to_dataset(object_key):
    test_bucket='domino-acme-test-bucket'
    read_bucket_profile_name='acme-read-bucket-role'    
    print(f"Copying key {object_key}")
    session = boto3.session.Session(profile_name=read_bucket_profile_name)
    s3_client = session.client('s3')
    data = s3_client.get_object(Bucket=test_bucket, Key=object_key)
    contents = data['Body'].read()
    print(f'\n---Contents of the key {object_key}----\n')
    print(contents.decode("utf-8"))
    project_name=os.environ['DOMINO_PROJECT_NAME']
    text_file = open(f"/domino/datasets/local/{project_name}/{object_key}", "w")
    n = text_file.write(str(contents))
    text_file.close()

import sys
    
if __name__ == "__main__":
    # S3 Object Jey
    object_key = sys.argv[1]
    print(object_key)
    ##Write new file to dataset
    #Read from S3 and copy to dataset
    now = datetime.now() # current date and time
    ## First create snapshot
    create_snapshot()
    ## Update the dataset
    write_to_dataset(object_key)
