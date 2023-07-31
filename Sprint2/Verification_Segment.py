import datetime
import sys
import pymongo
from Fonction import calcul_error, insertion_data


def getdaily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'bundle': {
                '$exists': True
            }
        }
    }, {
        '$lookup': {
            'from': 'segment', 
            'let': {
                'party_id': '$party_id'
            }, 
            'pipeline': [
                {
                    '$match': {
                        'day': '202212'
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$$party_id', '$party_id'
                            ]
                        }
                    }
                }
            ], 
            'as': 'segment'
        }
    }, {
        '$unwind': {
            'path': '$segment', 
            'includeArrayIndex': 's', 
            'preserveNullAndEmptyArrays': True
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
            '_id': '$segment.pot_segment_month', 
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

def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$segment', 
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
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(global_daily_usage,daily_usage,liste_segment):
    for i in range(len(liste_segment)):
        if liste_segment[i] in daily_usage and liste_segment[i] in global_daily_usage:
            daily_data = daily_usage[liste_segment[i]]
            global_data = global_daily_usage[liste_segment[i]]
            if not calcul_error(global_data,daily_data,1):
                print("Erreur de donne dans le segment "+liste_segment[i].__str__())
            else:
                print("Donne de "+liste_segment[i].__str__()+" verifie")
        elif liste_segment[i] in daily_usage and liste_segment[i] not in global_daily_usage:
            print(daily_usage[liste_segment[i]])
            print("Erreur de Donne de "+liste_segment[i].__str__()+" non existant dans global daily usage")
        
        elif liste_segment[i] not in daily_usage and liste_segment[i] in global_daily_usage:
            print(global_daily_usage[liste_segment[i]])
            print("Erreur de Donne de "+liste_segment[i].__str__()+" non existant dans daily usage")

        elif liste_segment[i] not in daily_usage and liste_segment[i] not in global_daily_usage:
            pass

if __name__ == "__main__":
    liste_segment = ["ZERO","SUPER LOW VALUE","LOW VALUE","MEDIUM","HIGH","SUPER HIGH VALUE","NEW","RETURN","CHURN","null"]
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_segment)