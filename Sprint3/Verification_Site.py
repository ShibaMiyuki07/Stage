from datetime import datetime
import sys
import pymongo
from Fonction import calcul_error, insertion_data
import mysql.connector

def getall_site():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3 where sig_nom_site is not null"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(site_name[0])
    all_site.append("null")
    return all_site

def getdaily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'topup': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$topup', 
            'includeArrayIndex': 't', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'tp', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$topup.recharge.site_name', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['daily_usage']
    retour={}
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour["null"] = insertion_data(r)
    return retour
    
def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'topup'
        }
    }, {
        '$group': {
            '_id': '$site_name', 
            'rec_cnt': {
                '$sum': '$rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$rec_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['global_daily_usage']
    retour={}
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour["null"] = insertion_data(r)
    return retour

def comparaison_donne(global_daily_usage,daily_usage,liste_site):
    for i in range(len(liste_site)):
        if liste_site[i] in global_daily_usage and liste_site[i] in daily_usage:
            global_data = global_daily_usage[liste_site[i]]
            daily_data = daily_usage[liste_site[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                print(error['data'])
                print('Erreur de donne a '+liste_site[i].__str__())
            else:
                pass
        elif liste_site[i] in global_daily_usage and liste_site[i] not in daily_usage:
            print(global_daily_usage[liste_site[i]])
            print('erreur donne de '+liste_site[i]+" inexistant dans daily usage")
        elif liste_site[i] in daily_usage and liste_site[i] not in global_daily_usage:
            print(daily_usage[liste_site[i]])
            print('erreur donne de '+liste_site[i].__str__()+" inexistant dans global daily usage")
        elif liste_site[i] not in daily_usage and liste_site[i] not in global_daily_usage:
            pass

    

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    liste_site = getall_site()
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_site)

