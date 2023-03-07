"""sys library used to process command line inputs"""
import sys
"""Hashing library used to store unique description values as ID numbers"""
import hashlib
"""JSON library used to load JSON files"""
import json
"""Elasticsearch library for storing and querying financial records"""
from elasticsearch import Elasticsearch, NotFoundError
"""Pandas library used to process and evaluate the JSON payloads"""
import pandas as pd
"""urllib3 dependency used by elasticsearch to communicate"""
import urllib3

sys.path.append('../')

"""Categorization library used to help classify financial records"""
import categorization
"""Contains required configuration variables"""
import config

urllib3.disable_warnings()

USER = config.USER
PASS = config.PASS
CERTIFICATE = config.CERTIFICATE

UNKNOWN_RANKINGS = {}
FUZZY_SUCCESSES = 0
es = Elasticsearch(
    hosts="https://localhost:9200",
    basic_auth=(USER, PASS),
    ca_certs=CERTIFICATE,
    verify_certs=False,
    ssl_show_warn=False
)

def json_to_dataframe(json_data):
    json_list = []

    for key in json_data:
        json_list.append(json_data[key])

    return categorize_dataframe(pd.DataFrame(json_list))

def description_to_unique_id(description: str):
    return int(hashlib.sha1(description.encode('utf-8')).hexdigest(), 16)

def create_new_document(index, description, category):
    doc = {
        'Original Description': description,
        'Parsed Description': description.replace(" ", ""),
        'Category': category
    }
    print(doc)
    resp = es.index(index=index, id=description_to_unique_id(description), document=doc)
    print(resp['result'])

def classify_description(description):
    resp = es.search(
        index="categorized_data", query = {
            "match_phrase_prefix": {
                "Description": {
                    "query": description
                }
            }
        }
    )
    result = None
    if resp['hits']['total']['value'] > 0:
        result = resp['hits']['hits'][0]["_source"]["Category"]
    return result

def delete_index(index_name):
    es.options(ignore_status=[400,404]).indices.delete(index=index_name)

def categorize_dataframe(dataframe):
    global FUZZY_SUCCESSES, UNKNOWN_RANKINGS

    # Iterate through the dataframe
    for index in dataframe.index:
        # Check if this description already exists
        description_exists = True
        try:
            resp = es.get(
                index='categorized_data',
                id=description_to_unique_id(dataframe['Description'][index]))
        except NotFoundError:
            description_exists = False

        if description_exists:
            # If the description already exists, assign the corresponding category and continue
            dataframe.at[index, 'Description'] = "%(Category)s" % resp['_source']
        else:
            # If the description does not exist, we will fuzzy query to get a category
            category = classify_description(dataframe['Description'][index])
            if category:
                dataframe.at[index, 'Category'] = category
                FUZZY_SUCCESSES += 1
            else:
                dataframe.at[index, 'Category'] = "UNKNOWN"
                try:
                    UNKNOWN_RANKINGS[dataframe['Description'][index]] += 1
                except KeyError:
                    UNKNOWN_RANKINGS[dataframe['Description'][index]] = 1

    return dataframe

# Opening JSON file
f = open('../../tests/data/json_credit_history.json')
json = json.load(f)

if len(sys.argv) == 2:
    if sys.argv[1] == 'process_data':
        dataframe = json_to_dataframe(json)
        categorization.categorization_report(dataframe, FUZZY_SUCCESSES, UNKNOWN_RANKINGS)
    elif sys.argv[1] == 'delete_index':
        delete_index('categorized_data')
