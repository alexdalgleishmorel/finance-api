import sys

sys.path.append('../')
import config
import categorization

import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError
import hashlib
import urllib3
urllib3.disable_warnings()

import csv
import json

USER = config.USER
PASS = config.PASS
CERTIFICATE = config.CERTIFICATE

unknown_rankings = {}
fuzzy_successes = 0
es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False, ssl_show_warn=False)

def json_to_dataframe(json_data):
    
    json_list = []

    for key in json_data:
        json_list.append(json_data[key])

    dataframe = categorize_dataframe(pd.DataFrame(json_list))

    return dataframe

def search_for_exact_description(description):
    try:
        resp = es.get(index='categorized_data', id=description_to_unique_id(description))
        print(resp)  
    except NotFoundError:
        description_exists = False

def description_to_unique_id(description: str):
    return int(hashlib.sha1(description.encode('utf-8')).hexdigest(), 16)

def search_all():
    resp = es.search(index="categorized_data", query={"match_all": {}})
    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("%(Description)s %(Category)s" % hit["_source"])

def create_new_document(index, description, category):
    doc = {
        'Original Description': description,
        'Parsed Description': description.replace(" ", ""),
        'Category': category
    }
    print(doc)
    resp = es.index(index=index, id=description_to_unique_id(description), document=doc)
    print(resp['result'])

def fuzzy_query(description):
    resp = es.search(index="categorized_data", query={
        "match_phrase_prefix": {
            "Description": {
                "query": description
            }
        }
    }
    )
    return resp

def delete_index(index_name):
    es.options(ignore_status=[400,404]).indices.delete(index=index_name)

def categorize_dataframe(dataframe):
    global fuzzy_successes, unknown_rankings

    # Iterate through the dataframe
    for index in dataframe.index:
        # Check if this description already exists
        description_exists = True
        try:
            resp = es.get(index='categorized_data', id=description_to_unique_id(dataframe['Description'][index]))       
        except NotFoundError:
            description_exists = False

        if description_exists:
            # If the description already exists, we can assign the corresponding category and continue
            dataframe.at[index, 'Description'] = "%(Category)s" % resp['_source']
            #print("%s already exists, using %s" % (dataframe['Description'][index], resp['_source']['Category']))
        else:
            # If the description does not exist, we will fuzzy query to get a category
            resp = fuzzy_query(dataframe['Description'][index])
            if resp['hits']['total']['value'] > 0:
                # IF GOOD FUZZY: Assign the corresponding category to dataframe row, then add the new description/category pair to the dataset
                dataframe.at[index, 'Category'] = "%(Category)s" % resp['hits']['hits'][0]["_source"]
                fuzzy_successes += 1
                print("Matched %s to %s" % (dataframe['Description'][index], resp['hits']['hits'][0]["_source"]['Description']))
            else:
                resp = fuzzy_query(dataframe['Description'][index].replace(" ", ""))
                if resp['hits']['total']['value'] > 0:
                    # IF GOOD FUZZY: Assign the corresponding category to dataframe row, then add the new description/category pair to the dataset
                    dataframe.at[index, 'Category'] = "%(Category)s" % resp['hits']['hits'][0]["_source"]
                    fuzzy_successes += 1
                    print("Matched %s to %s" % (dataframe['Description'][index], resp['hits']['hits'][0]["_source"]['Description']))
                else:
                    # IF BAD FUZZY: Write out a bad fuzzy report, assign UNKNOWN to this dataframe row
                    dataframe.at[index, 'Category'] = "UNKNOWN"
                    try:
                        unknown_rankings[dataframe['Description'][index]] += 1
                    except KeyError:
                        unknown_rankings[dataframe['Description'][index]] = 1
                    #print(f"Fuzzy for {dataframe['Description'][index]} turned up nothing")

    return dataframe

# Opening JSON file
f = open('../../tests/data/json_credit_history.json')
  
# returns JSON object as 
# a dictionary
json_data = json.load(f)

if len(sys.argv) == 2:
    if sys.argv[1] == 'process_data':
        dataframe = json_to_dataframe(json_data)
        categorization.categorization_report(dataframe, fuzzy_successes, unknown_rankings)
    elif sys.argv[1] == 'delete_index':
        delete_index('categorized_data')
elif len(sys.argv) == 3:
    if sys.argv[1] == 'exact_match':
        search_for_exact_description(sys.argv[2])
    elif sys.argv[1] == 'fuzzy_match':
        resp = fuzzy_query(es, sys.argv[2])
        print("Got %d Hits:" % resp['hits']['total']['value'])
        for hit in resp['hits']['hits']:
            print("%(Description)s %(Category)s" % hit["_source"])
