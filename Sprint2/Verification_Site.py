import codecs
from datetime import datetime
import sys
import mysql.connector
import pymongo

from Fonction import calcul_error, insertion_data, verification_cause

def getall_site():
    connexion = mysql.connector.connect(user='root',password='ShibaMiyuki07!',host='127.0.0.1',database='manitra')
    cursor = connexion.cursor() 
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3 where sig_nom_site is not null"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(codecs.encode(site_name[0],"UTF-8"))
    all_site.append("null")
    print("Site extracte")
    return all_site

def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$site_name', 
            'bndle_cnt': {
                '$sum': '$bndle_cnt'
            }, 
            'bundle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['global_daily_usage']
    retour = {}
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[codecs.encode(r['_id'],"UTF-8")] = insertion_data(r)
        else :
            retour['null'] = insertion_data(r)
    return retour



def getdaily_usage(client,day):
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
    }, {
        '$group': {
            '_id': '$bundle.site_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bundle_amnt': {
                '$sum': '$bundle.subscription.bndle_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['daily_usage']
    retour = {}
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[codecs.encode(r['_id'],"UTF-8")] = insertion_data(r)
        else :
            retour['null'] = insertion_data(r)
    return retour


def comparaison_donne(global_daily_usage,daily_usage,liste_site,client,day):
    for i in range(len(liste_site)):
        if liste_site[i] in daily_usage and liste_site[i] in global_daily_usage:
            global_data = global_daily_usage[liste_site[i]]
            daily_data = daily_usage[liste_site[i]]
            if not calcul_error(global_data,daily_data,1):
                print("Erreur de donne a "+liste_site[i].__str__())
                verification_cause(client,day,liste_site[i])
            else:
                print("Donne de "+liste_site[i].__str__()+"valide")
    

    

if __name__=="__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_site = getall_site()
    daily_usage = getdaily_usage(client,day)
    global_daily_usage = getglobal_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_site,client,day)