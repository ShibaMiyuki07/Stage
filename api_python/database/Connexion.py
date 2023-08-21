import pymongo


def connexion_base():
    '''try:
        client = pymongo.MongoClient("mongodb://192.168.56.102:27017")
        return client
    except:'''
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
    return client

        
        
def getverification_collection():
    client = connexion_base()
    db = client['cbm']
    collection = db['daily_usage_verification']
    return collection

def get_aggregation():
    client = connexion_base()
    db = client['cbm']
    collection = db['daily_aggregation']
    return collection