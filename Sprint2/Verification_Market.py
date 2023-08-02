import os
import sys
from Utils import calcul_error, date_to_datetime, getcollection_daily_aggrege, insertion_data, insertion_database


def getdaily_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day,
            'usage_type' : 'bundle',
            'type_aggregation' : "market"
        }
    },
    {
        '$project' : {
            '_id' : '$market',
            'bndle_cnt' : 1,
            'bndle_amnt' : 1
        }
    }
]
    collection = getcollection_daily_aggrege()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def getglobal_usage(day):
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
            'bndle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    collection = getcollection_daily_aggrege()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_market,day):
    donne_erreur = {}
    nbr_erreur = 0
    data = []
    donne_erreur['day'] = day
    donne_erreur['usage_type'] = 'bundle'
    for i in range(len(liste_market)):
        if liste_market[i] in daily_usage and liste_market[i] in global_daily_usage:
            global_data = global_daily_usage[liste_market[i]]
            daily_data = daily_usage[liste_market[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                data.append({"market" : liste_market[i],'error' : error['data'],'description' : 0})
            else:
                pass

        elif liste_market[i] in daily_usage and liste_market[i] not in global_daily_usage:
            nbr_erreur += 1
            data.append({'market' : liste_market[i],'error' : daily_usage[liste_market[i]],'description' : -1})
        elif liste_market[i] not in daily_usage and liste_market[i] in global_daily_usage:
            nbr_erreur += 1
            data.append({'market' : liste_market[i],'error' : global_daily_usage[liste_market[i]],'description' : 1})
        elif liste_market[i] not in daily_usage and liste_market[i] not in global_daily_usage:
            pass

    if nbr_erreur != 0:
        donne_erreur['erreur_market_cnt'] = nbr_erreur
        donne_erreur['erreur_market'] = data
        insertion_database(day,donne_erreur)  

    cmd = "python Verification_Segment.py "+sys.argv[1]
    os.system(cmd)

if __name__ == "__main__":
    liste_market = ["B2B","B2C"]
    day = date_to_datetime(sys.argv[1])
    daily_usage = getdaily_usage(day)
    global_daily_usage = getglobal_usage(day)
    comparaison_donne(daily_usage,global_daily_usage,liste_market,day)
