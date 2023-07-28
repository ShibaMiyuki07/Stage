
from datetime import datetime
import sys
import pymongo

from fonction_usage import calcul_error_usage, data_daily_usage, data_usage_global

def getdata_from_lookup(client,day):
    db = client['cbm']
    collection = db['daily_usage']
    last_month = day.year.__str__()+day.month.__str__()
    if day.month == 1:
        date_to_use = datetime(day.year-1,12,1)
        last_month = date_to_use.year.__str__()+date_to_use.month.__str__()
    else:
        date_to_use = datetime(day.year,day.month-1,1)
        if date_to_use.month <10:
            last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
        else:
            last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'u', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$usage.usage_op', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$group': {
            '_id': '$party_id', 
            'sms_i_cnt': {
                '$sum': '$usage.sms_i_cnt'
            }, 
            'voice_i_cnt': {
                '$sum': '$usage.voice_i_cnt'
            }, 
            'voice_i_vol': {
                '$sum': '$usage.voice_i_vol'
            }, 
            'voice_i_amnt': {
                '$sum': '$usage.voice_i_amnt'
            }, 
            'voice_o_cnt': {
                '$sum': '$usage.usage_op.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$usage.usage_op.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$usage.usage_op.voice_o_amnt'
            }, 
            'voice_o_bndle_vol': {
                '$sum': '$usage.usage_op.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$usage.usage_op.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$usage.usage_op.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$usage.usage_op.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$usage.usage_op.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$usage.usage_op.data_amnt'
            }, 
            'usage_2G': {
                '$sum': '$usage.usage_op.usage_2G'
            }, 
            'usage_3G': {
                '$sum': '$usage.usage_op.usage_3G'
            }, 
            'usage_4G_TDD': {
                '$sum': '$usage.usage_op.usage_4G_TDD'
            }, 
            'usage_4G_FDD': {
                '$sum': '$usage.usage_op.usage_4G_FDD'
            }, 
            'usage_4G_4G+': {
                '$sum': '$usage.usage_op.usage_4G_4G+'
            }, 
            'data_bndl_vol': {
                '$sum': '$usage.usage_op.data_bndl_vol'
            }, 
            'voice_vas_cnt': {
                '$sum': '$usage.usage_op.voice_vas_cnt'
            }, 
            'voice_vas_amnt': {
                '$sum': '$usage.usage_op.voice_vas_amnt'
            }, 
            'voice_vas_main_vol': {
                '$sum': '$usage.usage_op.voice_vas_main_vol'
            }, 
            'voice_vas_bndl_vol': {
                '$sum': '$usage.usage_op.voice_vas_bndl_vol'
            }, 
            'sms_vas_cnt': {
                '$sum': '$usage.usage_op.sms_vas_cnt'
            }, 
            'sms_vas_bndl_cnt': {
                '$sum': '$usage.usage_op.sms_vas_bndl_cnt'
            }, 
            'sms_vas_amnt': {
                '$sum': '$usage.usage_op.sms_vas_amnt'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'party_id': '$_id', 
            'sms_i_cnt': 1, 
            'voice_i_cnt': 1, 
            'voice_i_vol': 1, 
            'voice_i_amnt': 1, 
            'voice_o_cnt': 1, 
            'voice_o_main_vol': 1, 
            'voice_o_amnt': 1, 
            'voice_o_bndle_vol': 1, 
            'sms_o_main_cnt': 1, 
            'sms_o_bndl_cnt': 1, 
            'sms_o_amnt': 1, 
            'data_main_vol': 1, 
            'data_amnt': 1, 
            'usage_2G': 1, 
            'usage_3G': 1, 
            'usage_4G_TDD': 1, 
            'usage_4G_FDD': 1, 
            'usage_4G_4G+': 1, 
            'data_bndl_vol': 1, 
            'voice_vas_cnt': 1, 
            'voice_vas_amnt': 1, 
            'voice_vas_main_vol': 1, 
            'voice_vas_bndl_vol': 1, 
            'sms_vas_cnt': 1, 
            'sms_vas_bndl_cnt': 1, 
            'sms_vas_amnt': 1
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
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$$party_id', '$party_id'
                                    ]
                                }, {
                                    '$eq': [
                                        '$day', last_month
                                    ]
                                }
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
        '$group': {
            '_id': '$segment.vbs_Segment_month', 
            'sms_i_cnt': {
                '$sum': '$sms_i_cnt'
            }, 
            'voice_i_cnt': {
                '$sum': '$voice_i_cnt'
            }, 
            'voice_i_vol': {
                '$sum': '$voice_i_vol'
            }, 
            'voice_i_amnt': {
                '$sum': '$voice_i_amnt'
            }, 
            'voice_o_cnt': {
                '$sum': '$voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$voice_o_amnt'
            }, 
            'voice_o_bndle_vol': {
                '$sum': '$voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$data_amnt'
            }, 
            'usage_2G': {
                '$sum': '$usage_2G'
            }, 
            'usage_3G': {
                '$sum': '$usage_3G'
            }, 
            'usage_4G_TDD': {
                '$sum': '$usage_4G_TDD'
            }, 
            'usage_4G_FDD': {
                '$sum': '$usage_4G_FDD'
            }, 
            'usage_4G_4G+': {
                '$sum': '$usage_4G_4G+'
            }, 
            'data_bndl_vol': {
                '$sum': '$data_bndl_vol'
            }, 
            'voice_vas_cnt': {
                '$sum': '$voice_vas_cnt'
            }, 
            'voice_vas_amnt': {
                '$sum': '$voice_vas_amnt'
            }, 
            'voice_vas_main_vol': {
                '$sum': '$voice_vas_main_vol'
            }, 
            'voice_vas_bndl_vol': {
                '$sum': '$voice_vas_bndl_vol'
            }, 
            'sms_vas_cnt': {
                '$sum': '$sms_vas_cnt'
            }, 
            'sms_vas_bndl_cnt': {
                '$sum': '$sms_vas_bndl_cnt'
            }, 
            'sms_vas_amnt': {
                '$sum': '$sms_vas_amnt'
            }
        }
    }
]
    resultat = collection.aggregate(pipeline,cursor={})
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = data_daily_usage(r)
        else:
            retour['null'] = data_daily_usage(r)
    return retour


def getglobal_daily_par_segment(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'usage'
        }
    }, {
        '$group': {
            '_id': '$segment', 
            'sms_i_cnt': {
                '$sum': '$sms_i_cnt'
            }, 
            'voice_i_cnt': {
                '$sum': '$voice_i_cnt'
            }, 
            'voice_i_vol': {
                '$sum': '$voice_i_vol'
            }, 
            'voice_i_amnt': {
                '$sum': '$voice_i_amnt'
            }, 
            'voice_o_cnt': {
                '$sum': '$voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$voice_o_amnt'
            }, 
            'voice_o_bndle_vol': {
                '$sum': '$voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$data_amnt'
            }, 
            'usage_2G': {
                '$sum': '$usage_2G'
            }, 
            'usage_3G': {
                '$sum': '$usage_3G'
            }, 
            'usage_4G_TDD': {
                '$sum': '$usage_4G_TDD'
            }, 
            'usage_4G_FDD': {
                '$sum': '$usage_4G_FDD'
            }, 
            'usage_4G_4G+': {
                '$sum': '$usage_4G_4G+'
            }, 
            'data_bndl_vol': {
                '$sum': '$data_bndl_vol'
            }, 
            'voice_vas_cnt': {
                '$sum': '$voice_vas_cnt'
            }, 
            'voice_vas_amnt': {
                '$sum': '$voice_vas_amnt'
            }, 
            'voice_vas_main_vol': {
                '$sum': '$voice_vas_main_vol'
            }, 
            'voice_vas_bndl_vol': {
                '$sum': '$voice_vas_bndl_vol'
            }, 
            'sms_vas_cnt': {
                '$sum': '$sms_vas_cnt'
            }, 
            'sms_vas_bndl_cnt': {
                '$sum': '$sms_vas_bndl_cnt'
            }, 
            'sms_vas_amnt': {
                '$sum': '$sms_vas_amnt'
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
            retour[r['_id']] = data_usage_global(r)
        else : 
            retour['null'] = data_usage_global(r)
    return retour


def comparaison_donne(daily_usage,global_daily_usage,liste_segment):
    for i in range(len(liste_segment)):
        if liste_segment[i] in daily_usage and liste_segment[i] in global_daily_usage:
            daily_data = daily_usage[liste_segment[i]]
            global_data = global_daily_usage[liste_segment[i]]
            if not calcul_error_usage(global_data,daily_data):
                print("Erreur de donne dans le segment "+liste_segment[i].__str__())
            else:
                print("Donne de "+liste_segment[i].__str__()+" verifie")
        elif liste_segment[i] in daily_usage and liste_segment[i] not in global_daily_usage:
            print("Donne de "+liste_segment[i].__str__()+" non existant dans global daily usage")
        
        elif liste_segment[i] not in daily_usage and liste_segment[i] in global_daily_usage:
            print("Donne de "+liste_segment[i].__str__()+" non existant dans daily usage")

        elif liste_segment[i] not in daily_usage and liste_segment[i] not in global_daily_usage:
            pass

if __name__ == '__main__':
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_segment = ["ZERO","SUPER LOW VALUE","LOW VALUE","MEDIUM","HIGH","SUPER HIGH VALUE","NEW","RETURN","CHURN","null"]
    daily_usage = getdata_from_lookup(client,day)
    global_daily_usage = getglobal_daily_par_segment(client,day)
    comparaison_donne(daily_usage,global_daily_usage,liste_segment)