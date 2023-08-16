from Utils import getcollection_daily_aggrege, getcollection_global, insertion_data


def getdata_daily(day,nom):
    pipeline =[
        {
            '$match': {
                'day': day,
                'usage_type': 'ec',
                'type_aggregation' : nom
            }
        },
        {
            '$project' : {
                '_id' : '$'+nom,
                'ec_loan' : 1,
                'ec_qty' : 1,
                'ec_fees' : 1,
                'ec_payback' : 1,
                'ca_reactivation' : 1
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
            'usage_type': 'ec'
        }
    }, {
        '$group': {
            '_id': '$'+nom, 
             'ec_loan': {
                '$sum': '$ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$ec_qty'
            }, 
            'ec_fees': {
                '$sum': '$ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$ec_payback'
            }, 
            'ca_reactivation': {
                '$sum': '$ca_reactivation'
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