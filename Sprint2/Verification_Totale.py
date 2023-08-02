import sys
from Utils import calcul_error, date_to_datetime, getcollection_daily_aggrege, getcollection_global, insertion_data


def getglobal_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$day', 
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
    retour = {}
    for r in resultat:
        retour[day] = insertion_data(r)
    return retour

def getdaily_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day,
            'usage_type': 'bundle',
            'type_aggregation' : "day"
        }
    },
    {
        '$project' : {
            '_id' : 'day',
            'bndle_cnt' : 1,
            'bndle_amnt' : 1
        }
    }
]
    collection = getcollection_daily_aggrege()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        retour[day] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,day):
    daily_data = daily_usage[day]
    global_data = global_daily_usage[day]
    error = calcul_error(global_data,daily_data,0)
    if not error['retour'] :
        print('Donne contenant erreur')
        print(error['data'])
    else:
        print('donne verifier') 

if __name__ == "__main__":
    day = date_to_datetime(sys.argv[1])
    print(day)
    global_daily_usage = getglobal_usage(day)
    daily_usage = getdaily_usage(day)
    comparaison_donne(daily_usage,global_daily_usage,day)