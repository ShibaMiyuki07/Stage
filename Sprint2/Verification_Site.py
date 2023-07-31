import codecs
from datetime import datetime
import os
import sys
import mysql.connector
import pymongo

from Fonction import calcul_error, getdata_lieu_daily_usage, insertion_data, verification_cause

def getall_site():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3 where sig_nom_site is not null"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(codecs.encode(site_name[0],"UTF-8"))
    all_site.append("null")
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
            'bndle_amnt': {
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
            '_id': '$bundle.subscription.site_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bndle_amnt': {
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
    nbr_erreur = 0
    for i in range(len(liste_site)):
        if liste_site[i] in daily_usage and liste_site[i] in global_daily_usage:
            global_data = global_daily_usage[liste_site[i]]
            daily_data = daily_usage[liste_site[i]]
            if not calcul_error(global_data,daily_data,1):
                nbr_erreur += 1
                print("Erreur de donne a "+liste_site[i].__str__())
                verification_cause(client,day,liste_site[i])
            else:
                pass
        elif liste_site[i] not in daily_usage and liste_site[i] in global_daily_usage:
            print(global_daily_usage[liste_site[i]])
            print("Erreur de donne "+liste_site[i].__str__()+" daily usage")
        elif liste_site[i] in daily_usage and liste_site[i] not in global_daily_usage:
            print(daily_usage[liste_site[i]])
            print(getdata_lieu_daily_usage(day,liste_site[i],client))
            print("Erreur de donne "+liste_site[i].__str__()+" global daily usage")
        elif liste_site[i] not in daily_usage and liste_site[i] not in global_daily_usage:
            pass

    if nbr_erreur ==0:
        cmd = "python Verification_Bundle.py "+sys.argv[1]
        os.system(cmd)

        
    

if __name__=="__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_site = getall_site()
    daily_usage = getdaily_usage(client,day)
    global_daily_usage = getglobal_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_site,client,day)