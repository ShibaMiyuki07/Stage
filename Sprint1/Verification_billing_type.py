from datetime import datetime
import sys
import pymongo
import mysql.connector
from fonction_usage import data_daily_usage,calcul_error_usage

def getListe_Billing_type():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_billing_type"
    cursor.execute(query)
    all_billing_type = []
    for(name) in cursor:
        all_billing_type.append(name)
    return all_billing_type

def getdata_daily_usage(client,day):
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
            '_id': "$billing_type", 
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
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = data_daily_usage(r)
        else:
            retour['null'] = data_daily_usage(r)
    return retour

def getglobal_daily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'usage'
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
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
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = data_daily_usage(r)
        else:
            retour['null'] = data_daily_usage(r)
    return retour

def comparaison_donne(global_daily_usage,daily_usage,liste_billing_type):
    for i in range(len(liste_billing_type)):
        if liste_billing_type[i][0] in daily_usage and liste_billing_type[i][0] in global_daily_usage:
          global_data = global_daily_usage[liste_billing_type[i][0]]
          daily_data = daily_usage[liste_billing_type[i][0]]
          if calcul_error_usage(global_data,daily_data) == False:
            print("Erreur de donne dans "+liste_billing_type[i][0])
          else:
            print("Donne dans "+liste_billing_type[i][0])  

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_billing_type = getListe_Billing_type()
    daily_usage = getdata_daily_usage(client,day)
    global_daily_usage = getglobal_daily_usage(client,day)
    comparaison_donne(daily_usage,global_daily_usage,liste_billing_type)