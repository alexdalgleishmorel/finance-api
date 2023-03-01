import pandas as pd
import sys
from elasticsearch import Elasticsearch, NotFoundError
import hashlib

USER='elastic'
PASS='iLd5dGiWFRJ*oWKhjD+Q'
CERTIFICATE='d950c49376fcbaed594d61bb5946706381f2caf560254df9542bd1ce2a9292ac'

COLUMN_NAMES = ['Date', 'Description', 'Value', 'Category']

def process_csv(filepath, training=False):
    
    debitsDataframe = pd.read_csv(filepath, names=COLUMN_NAMES)
    
    creditsDataframe = debitsDataframe[debitsDataframe['Value'] > 0] 

    for index in debitsDataframe.index:
        if debitsDataframe['Value'][index] > 0:
            debitsDataframe.drop(index, inplace=True)

    print(debitsDataframe[["Description","Category"]].to_string())

    #add_training_data(debitsDataframe[["Description","Category"]])

    dataframe = categorize_dataframe(debitsDataframe)

    print(dataframe.to_string())

    categorization_report(dataframe)

    #search_all()

    #fuzzy_query()

    #print(debitsDataframe['Description'][355])

    #es.options(ignore_status=[400,404]).indices.delete(index='test-index')

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

def categorize_dataframe(dataframe):
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=(USER, PASS), ca_certs=CERTIFICATE, verify_certs=False)

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
            dataframe['Category'][index] = "%(Category)s" % resp['_source']
        else:
            # If the description does not exist, we will fuzzy query to get a category
            resp = es.search(index="categorized_data", query={
                "fuzzy": {
                    "Description": {
                        "fuzziness": "AUTO",
                        "value": dataframe['Description'][index]
                    }
                }
            })
            if resp['hits']['total']['value'] > 0:
                # IF GOOD FUZZY: Assign the corresponding category to dataframe row, then add the new description/category pair to the dataset
                dataframe['Category'][index] = "%(Category)s" % resp['hits']['hits'][0]["_source"]
                print(f"Mapped {dataframe['Description'][index]} to {dataframe['Category'][index]}")
            else:
                # IF BAD FUZZY: Write out a bad fuzzy report, assign UNKNOWN to this dataframe row
                dataframe['Category'][index] = "UNKNOWN"
                print(f"Fuzzy for {dataframe['Description'][index]} turned up nothing :(")

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
