from Utils import getcollection_daily_aggrege, getcollection_global, insertion_data


def getdata_daily(day,nom):
    pipeline =[
        {
            '$match': {
                'day': day,
                'usage_type': 'e-rc',
                'type_aggregation' : nom
            }
        },
        {
            '$project' : {
                '_id' : '$'+nom,
                'nb' : 1,
                'nb_mois':1,
                'nb_semaine':1
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

def  getglobal_usage(day,nom):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'e-rc'
        }
    }, {
        '$group': {
            '_id': '$'+nom, 
             'nb': {
                '$sum': '$nb'
            },
            'nb_semaine': {
                '$sum': '$nb_semaine'
            },'nb_mois': {
                '$sum': '$nb_mois'
            },
        }
    }
]
    collection = getcollection_global()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour