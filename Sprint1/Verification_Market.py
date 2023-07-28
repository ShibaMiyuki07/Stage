from datetime import datetime
import sys

import pymongo
from fonction_usage import calcul_error_usage, data_daily_usage, data_usage_global


def global_daily_by_market(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'usage'
        }
    }, {
        '$group': {
            '_id': '$market', 
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
            'voice_o_bndl_vol': {
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
    resultat =  collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] == None:
            retour['null'] = data_usage_global(r)
        else : 
            retour[r['_id']] = data_usage_global(r)
    return retour

def daily_usage_by_market(client,day):
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
            '_id': "$market", 
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
            'voice_o_bndl_vol': {
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
    }
]
    db = client['cbm']
    collection = db['daily_usage']
    retour = {}
    resultat = collection.aggregate(pipeline,cursor={},allowDiskUse=True)
    for r in resultat:
        if r['_id'] == None:
            retour['null'] = data_daily_usage(r)
        else : 
           retour[r['_id']] = data_daily_usage(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_market):
    for i in range(len(liste_market)):
        if liste_market[i] in daily_usage and liste_market[i] in global_daily_usage:
            daily_data = daily_usage[liste_market[i]]
            global_data = global_daily_usage[liste_market[i]]
            if not calcul_error_usage(global_data,daily_data):
                print("Erreur de donne dans le market "+liste_market[i].__str__())
            else:
                print("Donne de "+liste_market[i].__str__()+" verifie")
        elif liste_market[i] in daily_usage and liste_market[i] not in global_daily_usage:
            print("Donne de "+liste_market[i].__str__()+" non existant dans global daily usage")
        
        elif liste_market[i] not in daily_usage and liste_market[i] in global_daily_usage:
            print("Donne de "+liste_market[i].__str__()+" non existant dans daily usage")

        elif liste_market[i] not in daily_usage and liste_market[i] not in global_daily_usage:
            pass


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = global_daily_by_market(client,day)
    daily_usage = daily_usage_by_market(client,day)
    liste_market = ["B2B","B2C","null"]
    comparaison_donne(global_daily_usage,daily_usage,liste_market)