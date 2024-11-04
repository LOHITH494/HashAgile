from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import pandas as pd

es = Elasticsearch(
    "http://localhost:8989",
    basic_auth=('elastic', '123456')
)

def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")


def indexData(p_collection_name, p_exclude_column):
    try:
        data = pd.read_csv('C:/Users/Admin/Downloads/Employee Sample Data 1.csv', encoding='utf-8')
    except UnicodeDecodeError:
        data = pd.read_csv('C:/Users/Admin/Downloads/Employee Sample Data 1.csv', encoding='ISO-8859-1')

    data = data.where(pd.notnull(data), None)

    for index, record in data.iterrows():
        record_dict = record.to_dict()

        for key, value in record_dict.items():
            if isinstance(value, float) and pd.isna(value):
                record_dict[key] = None

        if p_exclude_column in record_dict:
            del record_dict[p_exclude_column]

        try:
            employee_id = record_dict.get('Employee ID')
            if employee_id is not None:
                es.index(index=p_collection_name, id=employee_id, document=record_dict)
            else:
                print(f"Employee ID not found for record: {record_dict}")
        except Exception as e:
            print(f"Failed to index record {index}: {e}")

    print(f"Data indexed into '{p_collection_name}', excluding column '{p_exclude_column}'.")
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    print(f"Found {response['hits']['total']['value']} record(s) in '{p_collection_name}' where '{p_column_name}' is '{p_column_value}'.")
    return response['hits']['hits']

def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Employee count in '{p_collection_name}': {count}")
    return count

def delEmpById(p_collection_name, p_employee_id):
    try:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Employee with ID '{p_employee_id}' deleted successfully.")
    except NotFoundError:
        print(f"Employee with ID '{p_employee_id}' not found.")



def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    print(f"Department facets in '{p_collection_name}': {response['aggregations']['departments']['buckets']}")
    return response['aggregations']['departments']['buckets']

v_nameCollection = 'hash_lohith'
v_phoneCollection = 'hash_7295'

createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
getEmpCount(v_nameCollection)
delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)

