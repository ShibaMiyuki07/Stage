import datetime
import sys
import pymongo
from Fonction import calcul_error, insertion_data


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
            '_id': '$day', 
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
        retour[r['_id']] = insertion_data(r)
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
            '_id': '$day', 
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
        retour[r['_id']] = insertion_data(r)
    return retour

def comparaison_donne(global_daily_usage,daily_usage,day):
    global_data = global_daily_usage[day]
    daily_data = daily_usage[day]
    error = calcul_error(global_data,daily_data,1)
    if not error['error']:
        print('Erreur de donne')
    else:
        print('Donne valide par jour')

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,day)

