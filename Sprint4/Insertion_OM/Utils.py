import pymongo


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_aggrege():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_daily_aggregation']
    return collection

def getcollection_om_details():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_om_details']
    return collection

def insertion_data(collection,donne):
    collection.insert_many(donne)

def getcollection_for_insertion(nom_collection):
    client = connexion_base()
    db = client['test']
    collection = db[nom_collection]
    return collection