from Verification_Bundle import getsubs


def insertion_data(r):
    retour = {}
    retour['data'] = {
        'bndle_cnt' : r['bndle_cnt'],
        'bndle_amnt' : r['bndle_amnt']
    }
    return retour['data']


def calcul_error(global_data,daily_data,taux_erreur):
    liste_key = list(daily_data.keys())
    error = []
    for i in liste_key:
        if i in daily_data and i in global_data:
            ecart =global_data[i] - daily_data[i]
            erreur = 0.0
            if global_data[i] != 0:
                erreur =(float) (ecart/global_data[i])*100
            if abs(erreur) >taux_erreur:
                error.append([i,erreur,ecart])
    if len(error)>0:
        print(error)
        return False
    return True


def verification_cause(client,day,location):
    pipeline_daily_usage= [
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
    }, 
    {
        '$match' : {
            'bundle.subscription.site_name' : location
        }
    },
    {
        '$group': {
            '_id': '$bundle.bndle_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bundle.subscription.bndle_amnt'
            }
        }
    }
]
    
    pipeline_global = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle',
            'site_name' : location
        }
    }, {
        '$group': {
            '_id': '$bndle_name', 
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
    collection_daily = db['daily_usage']
    collection_global = db['global_daily_usage']

    resultat_daily = collection_daily.aggregate(pipeline_daily_usage,cursor={})
    resultat_global = collection_global.aggregate(pipeline_global,cursor={})
    donne_daily = {}
    donne_global = {}
    for r in resultat_daily:
        donne_daily[r['_id']] = insertion_data(r)
    for r in resultat_global:
        donne_global[r['_id']] = insertion_data(r)

    liste_subs = getsubs()

    for i in range(len(liste_subs)):
        if liste_subs[i] in donne_daily and liste_subs[i] in donne_global:
            daily_data = donne_daily[liste_subs[i]]
            global_data = donne_global[liste_subs[i]]
            if not calcul_error(daily_data,global_data,0) :
                print("Erreur sur "+liste_subs[i].__str__())
            else:
                pass
        elif liste_subs[i] in donne_daily and liste_subs[i] not in donne_global:
            print("Erreur Donne de "+liste_subs[i].__str__()+" inexistant dans global daily usage")
        elif liste_subs[i] not in donne_daily and liste_subs[i] in donne_global:
            print(donne_global[liste_subs[i]])
            print("Erreur Donne de "+liste_subs[i].__str__()+" inexistant dans daily usage")
        elif liste_subs[i] not in donne_daily and liste_subs[i] not in donne_global:
            pass