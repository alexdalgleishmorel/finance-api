import pandas as pd
import sys
from elasticsearch import Elasticsearch

USER='elastic'
PASS='iLd5dGiWFRJ*oWKhjD+Q'
CERTIFICATE='d950c49376fcbaed594d61bb5946706381f2caf560254df9542bd1ce2a9292ac'

COLUMN_NAMES = ['Date', 'Description', 'Value']
COLUMN_NAMES_TRAINING = ['Date', 'Description', 'Value', 'Category']

def process_csv(filepath, training=False):
    if training:
        debitsDataframe = pd.read_csv(filepath, names=COLUMN_NAMES_TRAINING)
    else:
        debitsDataframe = pd.read_csv(filepath, names=COLUMN_NAMES)
    
    creditsDataframe = debitsDataframe[debitsDataframe['Value'] > 0] 

    for index in debitsDataframe.index:
        if debitsDataframe['Value'][index] > 0:
            debitsDataframe.drop(index, inplace=True)

    print(debitsDataframe[["Description","Category"]].to_string())

    fuzzy_query()

    return debitsDataframe, creditsDataframe


def fuzzy_query():
    # Uploading the 359th index to the server
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

    resp = es.search(index="test-index", query={
        "fuzzy": {
            "Description": {
                "value": "Bitchen"
            }
        }
    })
    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("%(Description)s %(Category)s" % hit["_source"])
    

if len(sys.argv) == 2:
    process_csv('../../tests/data/trainingData.csv', True)
else:
    process_csv('../../tests/data/creditCardHistory.csv')
