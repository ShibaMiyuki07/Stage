from datetime import datetime
import os
import sys
import pymongo
from Fonction import calcul_error, insertion_data
import mysql.connector

def getListe_Billing_type():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_billing_type"
    cursor.execute(query)
    all_billing_type = []
    for(name) in cursor:
        all_billing_type.append(name [0])
    return all_billing_type

def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
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
            retour[r['_id']] = insertion_data(r)
        else:
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
            '_id': '$billing_type', 
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
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_bt):
    nbr_erreur = 0
    for i in range(len(liste_bt)):
        if liste_bt[i] in daily_usage and liste_bt[i] in global_daily_usage:
            daily_data = daily_usage[liste_bt[i]]
            global_data = global_daily_usage[liste_bt[i]]
            if not calcul_error(global_data,daily_data,1):
                nbr_erreur += 1
                print("Erreur de donne dans le market "+liste_bt[i].__str__())
            else:
                print("Donne de "+liste_bt[i].__str__()+" verifie")
        elif liste_bt[i] in daily_usage and liste_bt[i] not in global_daily_usage:
            print(daily_usage[liste_bt[i]])
            print("Erreur de Donne de "+liste_bt[i].__str__()+" non existant dans global daily usage")
        
        elif liste_bt[i] not in daily_usage and liste_bt[i] in global_daily_usage:
            print(global_daily_usage[liste_bt[i]])
            print("Erreur de Donne de "+liste_bt[i].__str__()+" non existant dans daily usage")

        elif liste_bt[i] not in daily_usage and liste_bt[i] not in global_daily_usage:
            pass

    if nbr_erreur == 0:
        cmd = "python Verification_Segment.py "+sys.argv[1]
        os.system(cmd)

if __name__ == "__main__":
    liste_bt = getListe_Billing_type()
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_bt)