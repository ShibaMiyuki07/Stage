from Utils import getcollection_daily_aggrege, getcollection_global, insertion_data


def getdata_daily(day,nom):
    pipeline =[
        {
            '$match': {
                'day': day,
                'usage_type': 'parc',
                'type_aggregation' : nom
            }
        },
        {
            '$project' : {
                '_id' : '$'+nom,
                'parc_FT' : 1,
                'activation' : 1,
                'reconnexion' : 1,
                'deconnexion' : 1,
                'parc_rec_1j' : 1
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
            'usage_type': 'parc'
        }
    }, {
        '$group': {
            '_id': '$'+nom, 
            'parc_FT': {
                '$sum': '$parc_FT'
            }, 
            'activation': {
                '$sum': '$activation'
            },
            'reconnexion': {
                '$sum': '$reconnexion'
            },
            'deconnexion': {
                '$sum': '$deconnexion'
            },
            'parc_rec_1j': {
                '$sum': '$parc_rec_1j'
            }
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