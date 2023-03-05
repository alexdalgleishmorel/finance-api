import sys

sys.path.append('../')
import config

import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError
import hashlib
import urllib3
urllib3.disable_warnings()

COLUMN_NAMES = ['Date', 'Description', 'Value', 'Category']
USER = config.USER
PASS = config.PASS
CERTIFICATE = config.CERTIFICATE

unknown_rankings = {}
fuzzy_successes = 0
es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False, ssl_show_warn=False)

def process_csv(filepath, training=False):
    
    debitsDataframe = pd.read_csv(filepath, names=COLUMN_NAMES)
    
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

def categorize_dataframe(dataframe):

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
                global fuzzy_successes
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
                    global unknown_rankings
                    try:
                        unknown_rankings[dataframe['Description'][index]] += 1
                    except KeyError:
                        unknown_rankings[dataframe['Description'][index]] = 1
                    #print(f"Fuzzy for {dataframe['Description'][index]} turned up nothing")

    return dataframe


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
    

if len(sys.argv) == 2:
    if sys.argv[1] == 'training_data':
        process_csv('../../tests/data/trainingData.csv', True)
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
