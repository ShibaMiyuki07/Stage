import pymongo
import mysql.connector

from Model.Utilisateur import Utilisateur


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

def connexion_sql():
    connexion = mysql.connector.connect(user='root',password='Manitra',host='127.0.0.1',database="dashboard")
    return connexion

def test_login(user : Utilisateur):
    query = "SELECT id FROM utilisateur WHERE username=%s AND PASSWORD =SHA1(%s)"
    connection = connexion_sql()
    cursor = connection.cursor()
    cursor.execute(query,(user.username,user.password))
    cnt = 0
    for (id) in cursor:
        cnt += 1
    return cnt