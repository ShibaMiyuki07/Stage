from datetime import datetime
import sys
from fonction_usage import data_daily_usage,data_usage_global,calcul_error_usage
import pymongo
import mysql.connector

def getop_code():
    connexion = mysql.connector.connect(user='',password='',host='127.0.0.1',database='WORK')
    cursor = connexion.cursor() 
    query = "select distinct(name) as op_code from rf_operator"
    cursor.execute(query)
    liste_op = []
    for(op_code) in cursor:
        liste_op.append(op_code)
    print("Liste des operateurs extracte")
    return liste_op


def getdata_null_location_daily_usage(client,day):
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
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$match': {
            'usage.usage_op.site_name': None
        }
    }, {
        '$group': {
            '_id': {
                'day': '$day', 
                'op_code': '$op_code', 
                'site_name': '$usage.usage_op.site_name'
            }, 
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
    }
]
    db = client['cbm']
    collection = db['daily_usage']
    resultat = collection.aggregate(pipeline,cursor={})
    retour = {}
    for r in resultat :
        retour[r['op_code']] = data_daily_usage(r)
        if r['op_code'] == None:
            
            retour['null'] = data_daily_usage(r)
    print("Data from daily usage extracted")
    return retour

def getdata_null_location_global_daily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'usage', 
            'site_name': None
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
    resultat = collection.aggregate(pipeline,cursor={})
    retour = {}
    for r in resultat:
        retour[r['op_code']] = data_usage_global(r)
    return retour

def comparaison_donne(global_daily_usage,daily_usage,all_op_code):
    for i in range(len(all_op_code)):
        if all_op_code[i] in daily_usage and all_op_code[i] in global_daily_usage:
            daily_data = daily_usage[all_op_code[i]]
            global_data = global_daily_usage[all_op_code[i]]
            if calcul_error_usage(global_data,daily_data) == False:
                print("Erreur de donne avec l'operateur "+all_op_code[i].__str__())
            print("Donne de "+all_op_code[i] +" verifie")
        
        elif all_op_code[i] in daily_usage and all_op_code[i] not in global_daily_usage:
            print('Donne de '+all_op_code[i].__str__()+" non present dans global daily usage")
        
        elif all_op_code[i] not in daily_usage and all_op_code[i] not in global_daily_usage:
            pass
        elif all_op_code[i] not in daily_usage and all_op_code[i] in global_daily_usage:
            print("Donne de "+all_op_code[i].__str__()+" non present dans daily usage")
    print("Verifiication termine")

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    print("Verification par op_code des valeurs nulles le "+day.__str__()+" enclenche")
    global_daily_usage = getdata_null_location_global_daily_usage(client,day)
    daily_usage = getdata_null_location_daily_usage(client,day)
    all_op_code = getop_code()
    comparaison_donne(global_daily_usage,daily_usage,all_op_code)
