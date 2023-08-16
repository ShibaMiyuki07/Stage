from Utils import getcollection_daily_aggrege, getcollection_global, insertion_data


def getdata_daily(day,nom):
    pipeline =[
        {
            '$match': {
                'day': day,
                'usage_type': 'roaming',
                'type_aggregation' : nom
            }
        },
        {
            '$project' : {
                '_id' : '$'+nom,
                'voice_o_cnt' : 1,
                'voice_o_bndl_vol':1,
                'voice_o_main_vol':1,
                'voice_o_amnt':1,
                'sms_o_main_cnt':1,
                'sms_o_bndl_cnt':1,
                'data_main_vol':1,
                'data_bndl_vol':1,
                'sms_o_amnt':1,
                'data_amnt':1
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
            'usage_type': 'roaming'
        }
    }, {
        '$group': {
            '_id': '$'+nom, 
           'voice_o_cnt' : {
               '$sum' : '$voice_o_cnt'
            },
            'voice_o_bndl_vol':{
               '$sum' : '$voice_o_bndl_vol'
            },
            'voice_o_main_vol':{
               '$sum' : '$voice_o_main_vol'
            },
            'voice_o_amnt':{
               '$sum' : '$voice_o_amnt'
            },
            'sms_o_main_cnt':{
               '$sum' : '$sms_o_main_cnt'
            },
            'sms_o_bndl_cnt':{
               '$sum' : '$sms_o_bndl_cnt'
            },
            'data_main_vol':{
               '$sum' : '$data_main_vol'
            },
            'data_bndl_vol':{
               '$sum' : '$data_bndl_vol'
            },
            'sms_o_amnt':{
               '$sum' : '$sms_o_amnt'
            },
            'data_amnt':{
               '$sum' : '$data_amnt'
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