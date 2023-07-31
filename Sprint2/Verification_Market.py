from datetime import datetime
import sys
import pymongo
from Fonction import insertion_data,calcul_error


def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$market', 
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
        retour[day] = insertion_data(r)
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
            '_id': '$market', 
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
        retour[day] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_market):
    for i in range(len(liste_market)):
        if liste_market[i] in daily_usage and liste_market[i] in global_daily_usage:
            daily_data = daily_usage[liste_market[i]]
            global_data = global_daily_usage[liste_market[i]]
            if not calcul_error(global_data,daily_data,1):
                print("Erreur de donne dans le market "+liste_market[i].__str__())
            else:
                print("Donne de "+liste_market[i].__str__()+" verifie")
        elif liste_market[i] in daily_usage and liste_market[i] not in global_daily_usage:
            print(daily_usage[liste_market[i]])
            print("Erreur de Donne de "+liste_market[i].__str__()+" non existant dans global daily usage")
        
        elif liste_market[i] not in daily_usage and liste_market[i] in global_daily_usage:
            print(global_daily_usage[liste_market[i]])
            print("Erreur de Donne de "+liste_market[i].__str__()+" non existant dans daily usage")

        elif liste_market[i] not in daily_usage and liste_market[i] not in global_daily_usage:
            pass

if __name__ == "__main__":
    liste_market = ["B2B","B2C","null"]
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_market)