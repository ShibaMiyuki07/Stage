import pymongo
import sys
from datetime import datetime
from  decimal import Decimal

def getTotal_usage_jour_global_daily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'usage'
        }
    }, {
        '$group': {
            '_id': '$day', 
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
    resultat = collection.aggregate(pipeline,cursor={})
    retour = {}
    for r in resultat:
        retour[day.__str__()] = { 'sms_i_cnt' : r['sms_i_cnt'],
                                 'voice_i_cnt' : r['voice_i_cnt'],
                                 'voice_i_vol' : r['voice_i_vol'],
                                 'voice_i_amnt' : r['voice_i_amnt'],
                                 'voice_i_cnt' : r['voice_i_amnt'],
                                 'voice_o_cnt' : r['voice_o_cnt'],
                                 'voice_o_main_vol' : r['voice_o_main_vol'],
                                 'voice_o_amnt' : r['voice_o_amnt'],
                                 'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
                                 'sms_o_main_cnt' : r['sms_o_main_cnt'],
                                 'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
                                 'sms_o_amnt' : r['sms_o_amnt'],
                                 'data_main_vol' : r['data_main_vol'],
                                 'data_amnt' : r['data_amnt'],
                                 'usage_2G' : r['usage_2G'],
                                 'usage_3G' : r['usage_3G'],
                                 'usage_4G_TDD' : r['usage_4G_TDD'],
                                 'usage_4G_FDD' : r['usage_4G_FDD'] + r['usage_4G_4G+'],
                                 'data_bndl_vol' : r['data_bndl_vol'],
                                 'voice_vas_cnt' : r['voice_vas_cnt'],
                                 'voice_vas_amnt' :r['voice_vas_amnt'],
                                 'voice_vas_main_vol' : r['voice_vas_main_vol'],
                                 'voice_vas_bndl_vol' : r['voice_vas_bndl_vol'],
                                 'sms_vas_cnt' : r['sms_vas_cnt'],
                                 'sms_vas_bndl_cnt' : r['sms_vas_bndl_cnt'],
                                 'sms_vas_amnt' : r['sms_vas_amnt']  }
    print("Total global_daily_usage du "+day.__str__()+" extracte avec succes")
    return retour

def getTotal_usage_jour_daily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'u', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$unwind': {
            'path': '$usage.usage_op', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$group': {
            '_id': '$day', 
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
    resultat = collection.aggregate(pipeline,cursor={})
    retour = {}
    for r in resultat:
        retour[day.__str__()] = { 'sms_i_cnt' : r['sms_i_cnt'],
                                 'voice_i_cnt' : r['voice_i_cnt'],
                                 'voice_i_vol' : r['voice_i_vol'],
                                 'voice_i_amnt' : r['voice_i_amnt'],
                                 'voice_i_cnt' : r['voice_i_amnt'],
                                 'voice_o_cnt' : r['voice_o_cnt'],
                                 'voice_o_main_vol' : r['voice_o_main_vol'],
                                 'voice_o_amnt' : r['voice_o_amnt'],
                                 'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
                                 'sms_o_main_cnt' : r['sms_o_main_cnt'],
                                 'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
                                 'sms_o_amnt' : r['sms_o_amnt'],
                                 'data_main_vol' : r['data_main_vol'],
                                 'data_amnt' : r['data_amnt'],
                                 'usage_2G' : r['usage_2G'],
                                 'usage_3G' : r['usage_3G'],
                                 'usage_4G_TDD' : r['usage_4G_TDD'],
                                 'usage_4G_FDD' : r['usage_4G_FDD'],
                                 'data_bndl_vol' : r['data_bndl_vol'],
                                 'voice_vas_cnt' : r['voice_vas_cnt'],
                                 'voice_vas_amnt' :r['voice_vas_amnt'],
                                 'voice_vas_main_vol' : r['voice_vas_main_vol'],
                                 'voice_vas_bndl_vol' : r['voice_vas_bndl_vol'],
                                 'sms_vas_cnt' : r['sms_vas_cnt'],
                                 'sms_vas_bndl_cnt' : r['sms_vas_bndl_cnt'],
                                 'sms_vas_amnt' : r['sms_vas_amnt']
                                   }
    print("Total daily_usage du "+day.__str__()+" extracte avec succes")
    return retour

def comparaison_donne(global_daily_usage,daily_usage,day):
    global_data = global_daily_usage[day.__str__()]
    daily_data = daily_usage[day.__str__()]
    voice_i_amnt_ecart = global_data['voice_i_amnt'] - daily_data['voice_i_amnt']
    voice_o_amnt_ecart = global_data['voice_o_amnt'] - daily_data['voice_o_amnt']
    sms_o_amnt_ecart = global_data['sms_o_amnt'] - daily_data['sms_o_amnt']
    data_amnt_ecart = global_data['data_amnt'] - daily_data['data_amnt']
    voice_vas_amnt_ecart = global_data['voice_vas_amnt'] - daily_data['voice_vas_amnt']
    value = [voice_i_amnt_ecart,voice_o_amnt_ecart,sms_o_amnt_ecart,data_amnt_ecart,voice_vas_amnt_ecart]
    print(value)
    voice_i_amnt_error = Decimal(0)
    voice_o_amnt_error = Decimal(0)
    sms_o_amnt_error = Decimal(0)
    data_amnt_error = Decimal(0)
    voice_vas_amnt_error = Decimal(0)


    #Calcul du pourcentage d'erreur
    if voice_i_amnt_ecart != 0:
        voice_i_amnt_error = (Decimal(voice_i_amnt_ecart.__str__()) /Decimal(global_data['voice_i_amnt'].__str__())) * Decimal(100)
    if global_data['voice_o_amnt'] != 0:
        voice_o_amnt_error = (Decimal(voice_o_amnt_ecart.__str__()) /Decimal(global_data['voice_o_amnt'].__str__())) * Decimal(100)
    if global_data['sms_o_amnt'] != 0:
        sms_o_amnt_error = (Decimal(sms_o_amnt_ecart.__str__()) /Decimal(global_data['sms_o_amnt'].__str__())) * Decimal(100) 
    if global_data['data_amnt'] != 0:
        data_amnt_error = (Decimal(data_amnt_ecart.__str__()) /Decimal(global_data['data_amnt'].__str__())) * Decimal(100)
    if global_data['voice_vas_amnt'] != 0:
        voice_vas_amnt_error = (Decimal(voice_vas_amnt_ecart.__str__())/Decimal(global_data['voice_vas_amnt'].__str__())) * Decimal(100)
    value_error = [voice_i_amnt_error,voice_o_amnt_error,sms_o_amnt_error,data_amnt_error,voice_vas_amnt_error]
    print(value_error)

  

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    print(day)
    global_daily_usage = getTotal_usage_jour_daily_usage(client,date_time)
    daily_usage = getTotal_usage_jour_global_daily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,day)
    