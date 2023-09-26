import pymongo
import mysql.connector

from Model.Utilisateur import Utilisateur


def connexion_base():
    '''try:
        client = pymongo.MongoClient("mongodb://192.168.56.102:27017")
        return client
    except:'''
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

        
        
def getverification_collection():
    client = connexion_base()
    db = client['test']
    collection = db['daily_usage_verification']
    return collection

def get_aggregation():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_daily_aggregation']
    return collection

def connexion_sql():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database="DWH")
    return connexion

def test_login(user : Utilisateur):
    query = "SELECT id FROM utilisateur_dashboard WHERE identifiant=%s AND PASSWORD =SHA1(%s)"
    connection = connexion_sql()
    cursor = connection.cursor()
    cursor.execute(query,(user.username,user.password))
    cnt = 0
    for (id) in cursor:
        cnt += 1
    return cnt
