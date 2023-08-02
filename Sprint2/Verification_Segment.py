import sys
from Utils import calcul_error, date_to_datetime, getcollection_daily_aggrege, insertion_data, insertion_database


def getdaily_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day,
            'usage_type' : 'bundle',
            'type_aggregation' : "segment"
        }
    },
    {
        '$project' : {
            '_id' : '$segment',
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
            '_id': '$segment', 
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

def comparaison_donne(daily_usage,global_daily_usage,liste_segment,day):
    donne_erreur = {}
    nbr_erreur = 0
    data = []
    donne_erreur['day'] = day
    donne_erreur['usage_type'] = 'bundle'
    for i in range(len(liste_segment)):
        if liste_segment[i] in daily_usage and liste_segment[i] in global_daily_usage:
            global_data = global_daily_usage[liste_segment[i]]
            daily_data = daily_usage[liste_segment[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                data.append({"segment" : liste_segment[i],'error' : error['data'],'description' : 0})
            else:
                pass

        elif liste_segment[i] in daily_usage and liste_segment[i] not in global_daily_usage:
            nbr_erreur += 1
            data.append({'segment' : liste_segment[i],'error' : daily_usage[liste_segment[i]],'description' : -1})
        elif liste_segment[i] not in daily_usage and liste_segment[i] in global_daily_usage:
            nbr_erreur += 1
            data.append({'segment' : liste_segment[i],'error' : global_daily_usage[liste_segment[i]],'description' : 1})
        elif liste_segment[i] not in daily_usage and liste_segment[i] not in global_daily_usage:
            pass

    donne_erreur['erreur_segment_cnt'] = 0
    if nbr_erreur != 0:
        donne_erreur['erreur_segment_cnt'] = nbr_erreur
        donne_erreur['erreur_segment'] = data

    insertion_database(day,donne_erreur)  

if __name__ == "__main__":
    liste_segment = ["ZERO","SUPER LOW VALUE","LOW VALUE","MEDIUM","HIGH","SUPER HIGH VALUE","NEW","RETURN","CHURN","null"]
    day = date_to_datetime(sys.argv[1])
    daily_usage = getdaily_usage(day)
    global_daily_usage = getglobal_usage(day)
    comparaison_donne(daily_usage,global_daily_usage,liste_segment,day)
