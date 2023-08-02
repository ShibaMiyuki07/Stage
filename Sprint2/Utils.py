from datetime import datetime
import pymongo


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_usage():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_daily_aggregation']
    return collection

def getcollection_global():
    client =connexion_base()
    db = client['cbm']
    collection = db['global_daily_usage']
    return collection

def insertion_data(r):
    keys = list(r.keys())
    data = {}
    for i in keys:
        if i != "_id":
            data[i] = r[i]
    return data

def date_to_datetime(date):
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    return day


def calcul_error(global_data,daily_data,taux_erreur):
    liste_key = list(daily_data.keys())
    liste_error = []
    
    for i in liste_key:
        if i in daily_data and i in global_data:
            ecart =global_data[i] - daily_data[i]
            erreur = 0.0
            if global_data[i] != 0:
                erreur =(float) (ecart/global_data[i])*100
            if abs(erreur) >taux_erreur:
                error = {}
                error["nom"] = i
                error["error"] = erreur
                error['ecart'] = ecart
                liste_error.append(error)
    if len(liste_error)>0:
        return {"retour" : False,"data" : liste_error}
    return {"retour" : True}