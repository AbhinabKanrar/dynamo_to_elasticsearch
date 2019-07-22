import time
import boto3
import json
import sys
import schedule
import uuid
import json
import requests
import traceback
from dynamodb_json import json_util as d_json


dynamo_client = boto3.client('dynamodb',
                             aws_access_key_id=<access key>,
                             aws_secret_access_key=<secret key>,
                             region_name=<region>)


def fetch(table_name, paginator):
        return paginator.paginate(
            TableName=table_name,
            PaginationConfig={'PageSize': 1000}
        )

def migrateData(dynamo_client, table_name):
    result = []
    paginator = dynamo_client.get_paginator('scan')
    page_iterator = fetch(table_name, paginator)

    for page in page_iterator:
        if page['Items']:
            for item in page['Items']:
                result.append(d_json.loads(json.dumps(item)))

    for data in result:
        api = '<ES endpoint>/<index name>/_doc'

        requests.post(
            api,
            data=json.dumps(data),
            headers={
                'Content-Type': 'application/json'
            })


migrateData(dynamo_client, '<dynamo table name>')
