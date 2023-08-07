from Utils import getcollection_daily_aggrege, getcollection_global, insertion_data


def getdata_daily(day,nom):
    pipeline =[
        {
            '$match': {
                'day': day,
                'usage_type': 'om',
                'type_aggregation' : nom
            }
        },
        {
            '$project' : {
                '_id' : '$'+nom,
                'om_cnt' : 1,
                'om_amnt' : 1,
                'om_tr_amnt' : 1
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
            'usage_type': 'om'
        }
    }, {
        '$group': {
            '_id': '$'+nom, 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_amnt': {
                '$sum': '$om_amnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
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