import sys

sys.path.append('../')
import config

import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError
import hashlib
import urllib3
urllib3.disable_warnings()

import csv
import json

COLUMN_NAMES = ['Date', 'Description', 'Value', 'Category']
USER = config.USER
PASS = config.PASS
CERTIFICATE = config.CERTIFICATE

unknown_rankings = {}
fuzzy_successes = 0
es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False, ssl_show_warn=False)

def json_to_dataframe(json_data):
    
    debitsDataframe = pd.json_normalize(json_data)
    print(debitsDataframe)
    
    creditsDataframe = debitsDataframe[debitsDataframe['Value'] > 0] 

    for index in debitsDataframe.index:
        if debitsDataframe['Value'][index] > 0:
            debitsDataframe.drop(index, inplace=True)

    es.index(index='categorized_data', id=1, document={"init": "init"})

    dataframe = categorize_dataframe(debitsDataframe)

    #print(dataframe.to_string())

    categorization_report(dataframe)

    return debitsDataframe, creditsDataframe

def categorization_report(dataframe):
    total = 0
    success = 0
    unknown = 0

    for index in dataframe.index:
        if dataframe['Category'][index] != "UNKNOWN":
            success += 1
        else:
            unknown += 1
        total += 1
    
    print(f"Total successful matches: {success}/{total} --> {str(round((success/total)*100, 2))}%")
    print(f"Total unknowns: {unknown}/{total} --> {str(round((unknown/total)*100, 2))}%")
    global fuzzy_successes
    global unknown_rankings
    print("Fuzzy Successes: %s" % fuzzy_successes)
    rank_array = []
    for key in unknown_rankings:
        rank_array.append((key, unknown_rankings[key]))
    rank_array.sort(key = lambda x: x[1])
    rank_array.reverse()
    print(rank_array[0:5])

def add_training_data(dataframe):
    for index in dataframe.index:
        create_new_document('categorized_data', dataframe['Description'][index], dataframe['Category'][index])

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
    

# create a dictionary
data = {}
csvFilePath="../../tests/data/creditCardHistory.csv"
jsonFilePath="../../tests/data/json_credit_history.json"
    
# Open a csv reader called DictReader
with open(csvFilePath, encoding='utf-8') as csvf:
    csvReader = csv.DictReader(csvf)
        
    # Convert each row into a dictionary
    # and add it to data
    count = 0
    for rows in csvReader:
        row_count = 0
        for key in rows:
            if row_count == 0:
                rows['Date'] = rows.pop(key)
            elif row_count == 1:
                rows['Description'] = rows.pop(key)
            elif row_count == 2:
                rows['Value'] = rows.pop(key)
            row_count += 1
        data[str(count)] = rows
        count += 1

# Open a json writer, and use the json.dumps()
# function to dump data
with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
    jsonf.write(json.dumps(data, indent=4))

"""
if len(sys.argv) == 2:
    if sys.argv[1] == 'training_data':
        json_to_dataframe(json_data)
    elif sys.argv[1] == 'normal_data':
        process_csv('../../tests/data/creditCardHistory.csv')
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
"""
