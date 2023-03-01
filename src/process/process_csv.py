import pandas as pd
import sys
from elasticsearch import Elasticsearch
import hashlib

USER='elastic'
PASS='P*P1E-QpIl2J10yWdjbL'
CERTIFICATE='26e3d93b3bebc2ece036c2547f5cf3e88931a22712bc89121c70be167aaaa561'

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

    #add_training_data(debitsDataframe[["Description","Category"]])

    search_all()

    #fuzzy_query()

    #print(debitsDataframe['Description'][355])

    #es.options(ignore_status=[400,404]).indices.delete(index='test-index')

    return debitsDataframe, creditsDataframe

def categorize_dataframe(dataframe):
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

    # Iterate through the dataframe
    for index in dataframe.index:
        pass
        # Check if this description already exists

        # If the description already exists, we can assign the corresponding category and continue

        # If the description does not exist, we will fuzzy query to get a category

        # IF GOOD FUZZY: Assign the corresponding category to dataframe row, then add the new description/category pair to the dataset

        # IF BAD FUZZY: Write out a bad fuzzy report, assign UNKNOWN to this dataframe row

    return dataframe


def add_training_data(dataframe):
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

    for index in dataframe.index:
        create_new_document('categorized_data', dataframe['Description'][index], dataframe['Category'][index])


def description_to_unique_id(description: str):
    return int(hashlib.sha1(description.encode('utf-8')).hexdigest(), 16)

def search_all():
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

    resp = es.search(index="categorized_data", query={"match_all": {}})
    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("%(Description)s %(Category)s" % hit["_source"])

def create_new_document(index, description, category):
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)
    doc = {
        'Description': description,
        'Category': category
    }
    resp = es.index(index=index, id=description_to_unique_id(description), document=doc)
    print(resp['result'])

def fuzzy_query():
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

    resp = es.search(index="categorized_data", query={
        "fuzzy": {
            "Description": {
                "fuzziness": "AUTO",
                "value": "shawarma"
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
