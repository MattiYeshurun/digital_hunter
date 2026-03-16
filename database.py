from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['digital_hunter_db']
collection = db['targets']

def get_target(entity_id):
    return collection.find_one({"entity_id": entity_id})

def upsert_target(entity_id, update_data):
    collection.update_one({"entity_id": entity_id}, 
                          {'$set': update_data}, upsert=True)



