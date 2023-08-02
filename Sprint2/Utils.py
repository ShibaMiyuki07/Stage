from datetime import datetime
import mysql.connector
import pymongo


def getsubs():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor()
    query = "select name from rf_subscriptions where name is not null"
    cursor.execute(query)
    list_subs = []
    for(name) in cursor:
        list_subs.append(name[0])
    list_subs.append('null')
    return list_subs


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_aggrege():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_daily_aggregation']
    return collection

def getcollection_global():
    client =connexion_base()
    db = client['cbm']
    collection = db['global_daily_usage']
    return collection

def getcollection_daily_usage():
    client = connexion_base()
    db = client['cbm']
    collection = db['daily_usage']
    return collection

def getcollection_for_insertion():
    client = connexion_base()
    db = client['test']
    collection = db['daily_usage_verification']
    return collection

def insertion_data(r):
    keys = list(r.keys())
    data = {}
    for i in keys:
        if i != "_id":
            data[i] = r[i]
    return data

def insertion_database(day,donne):
    collection = getcollection_for_insertion()
    resultat = collection.find({'day' : day,'usage_type' : 'bundle'})
    count = 0
    for r in resultat:
        count += 1
    
    if count>0:
        list_key = list(donne.keys())
        for r in list_key:
            collection.update_one({'day' : day,'usage_type' : 'bundle'},{"$set" : {r : donne[r]}})
        
    else:
        collection.insert_one(donne)

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


#Voir les causes des erreurs par site
def verification_cause(day,location):
    pipeline_daily_usage = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$bundle', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$bundle.subscription', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, 
    {
        '$match' : {
            'bundle.subscription.site_name' : location,
        }
    },

    {
        '$group': {
            '_id': '$bundle.bndle_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bundle.subscription.bndle_amnt'
            }
        }
    }
]
    
    pipeline_global_usage = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle',
            'site_name' : location
        }
    }, {
        '$group': {
            '_id': '$bndle_name', 
            'bndle_cnt': {
                '$sum': '$bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    collection_daily_usage = getcollection_daily_usage()
    collection_global_usage = getcollection_global()

    resultat_global = collection_global_usage.aggregate(pipeline_global_usage)
    resultat_daily = collection_daily_usage.aggregate(pipeline_daily_usage)
    donne_global = {}
    donne_daily = {}
    for r in resultat_global:
        if r['_id'] != None:
            donne_global[r['_id']] = insertion_data(r)
        else : 
            donne_global['null'] = insertion_data(r)

    for r in resultat_daily:
        if r['_id'] != None:
            donne_daily[r['_id']] = insertion_data(r)
        else:
            donne_daily['null'] = insertion_data(r)

    liste_subs = getsubs()
    details = []
    for i in range(len(liste_subs)):
        if liste_subs[i] in donne_daily and liste_subs[i] in donne_global:
            daily_data = donne_daily[liste_subs[i]]
            global_data = donne_global[liste_subs[i]]

            error = calcul_error(global_data,daily_data,0)
            if not error['retour']:
                details.append({'bndle_name' : liste_subs[i],'donne' : error['data'],'description' : 0})
            else:
                pass

        elif liste_subs[i] in donne_daily and liste_subs[i] not in donne_global:
            details.append({'bndle_name' : liste_subs[i],'donne' : donne_daily[liste_subs[i]],'description' : -1})
        elif liste_subs[i] not in donne_daily and liste_subs[i] in donne_global:
            details.append({'bndle_name': liste_subs[i],'donne' : donne_global[liste_subs[i]],'description' : 1})
        elif liste_subs[i] not in donne_daily and liste_subs[i] not in donne_global:
            pass

    return details

def getdata_daily_usage(day,location):
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$bundle', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$bundle.subscription', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, 
    {
        '$match' : {
            'bundle.subscription.site_name' : location,
        }
    },

    {
        '$group': {
            '_id': '$bundle.bndle_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bundle.subscription.bndle_amnt'
            }
        }
    }
]
    retour = []
    collection = getcollection_daily_usage()
    resultat  = collection.aggregate(pipeline)
    for r in resultat:
        retour.append({'bndle_name' : r['_id'],'bndle_cnt' : r['bndle_cnt'] , 'bndle_amnt' : r['bndle_amnt']})
    return retour


def getdata_global(day,location):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle',
            'site_name' : location
        }
    }, {
        '$group': {
            '_id': '$bndle_name', 
            'bndle_cnt': {
                '$sum': '$bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    
    collection = getcollection_global()
    resultat = collection.aggregate(pipeline)
    retour = []
    for r in resultat:
        retour.append({'bndle_name' : r['_id'],'bndle_cnt':r['bndle_cnt'],'bndle_amnt' : r['bndle_amnt']})
    return retour