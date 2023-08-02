import pymongo


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_usage():
    client = connexion_base()
    db = client['cbm']
    collection = db['daily_usage']
    return collection

def getcollection_global():
    client =connexion_base()
    db = client['cbm']
    collection = db['global_daily_usage']
    return collection

def getcollection_insertion(nom_collection):
    client = connexion_base()
    db = client['test']
    collection = db[nom_collection]
    return collection

def insertion_data(collection,donne):
    collection.insert_many(donne)