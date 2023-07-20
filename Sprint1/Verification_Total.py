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

    #Ajout du resultat
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

    #Ajout du resultat
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

    #Prendre les donnees a partir des bibliotheques
    global_data = global_daily_usage[day.__str__()]
    daily_data = daily_usage[day.__str__()]

    #Calcul des ecarts entre les donnees de depart et les donnees finales
    sms_i_cnt_ecart = global_data['sms_i_cnt'] - daily_data['sms_i_cnt']
    voice_i_cnt_ecart = global_data['voice_i_cnt'] - daily_data['voice_i_cnt']
    voice_i_vol_ecart = global_data['voice_i_vol'] - daily_data['voice_i_vol']
    voice_o_cnt_ecart = global_data['voice_o_cnt'] - daily_data['voice_o_cnt']
    voice_o_main_vol_ecart = global_data['voice_o_main_vol'] - daily_data['voice_o_main_vol']
    voice_o_bndl_vol_ecart = global_data['voice_o_bndl_vol'] - daily_data['voice_o_bndl_vol']
    sms_o_main_cnt_ecart = global_data['sms_o_main_cnt'] - daily_data['sms_o_main_cnt']
    sms_o_bndl_cnt_ecart = global_data['sms_o_bndl_cnt'] - daily_data['sms_o_bndl_cnt']
    data_main_vol_ecart = global_data['data_main_vol'] - daily_data['data_main_vol']
    usage_2g_ecart = global_data['usage_2G'] - daily_data['usage_2G']
    usage_3g_ecart = global_data['usage_3G'] - daily_data['usage_3G']
    usage_4G_TDD_ecart = global_data['usage_4G_TDD'] - daily_data['usage_4G_TDD']
    usage_4G_FDD_ecart = global_data['usage_4G_FDD'] - daily_data['usage_4G_FDD']
    data_bndl_vol_ecart = global_data['data_bndl_vol'] - daily_data['data_bndl_vol']
    voice_vas_cnt_ecart = global_data['voice_vas_cnt'] - daily_data['voice_vas_cnt']
    voice_vas_main_vol_ecart = global_data['voice_vas_main_vol'] - daily_data['voice_vas_main_vol']
    voice_vas_bndl_vol_ecart = global_data['voice_vas_bndl_vol'] - daily_data['voice_vas_bndl_vol']
    sms_vas_cnt_ecart = global_data['sms_vas_cnt'] - daily_data['sms_vas_cnt']
    sms_vas_bndl_cnt_ecart = global_data['sms_vas_bndl_cnt'] - daily_data['sms_vas_bndl_cnt']

    #Ecart des revenus pa service
    voice_i_amnt_ecart = global_data['voice_i_amnt'] - daily_data['voice_i_amnt']
    voice_o_amnt_ecart = global_data['voice_o_amnt'] - daily_data['voice_o_amnt']
    sms_o_amnt_ecart = global_data['sms_o_amnt'] - daily_data['sms_o_amnt']
    data_amnt_ecart = global_data['data_amnt'] - daily_data['data_amnt']
    voice_vas_amnt_ecart = global_data['voice_vas_amnt'] - daily_data['voice_vas_amnt']
    sms_vas_amnt_ecart = global_data['sms_vas_amnt'] - daily_data['sms_vas_amnt']

    #Compiler les donnees dans un tableau
    value = [sms_i_cnt_ecart,
             voice_i_cnt_ecart,
             voice_i_vol_ecart,
             voice_i_amnt_ecart,
             voice_o_cnt_ecart,
             voice_o_main_vol_ecart,
             voice_o_amnt_ecart,
             voice_o_bndl_vol_ecart,
             sms_o_main_cnt_ecart,
             sms_o_bndl_cnt_ecart,
             sms_o_amnt_ecart,
             data_main_vol_ecart,
             data_amnt_ecart,
             usage_2g_ecart,
             usage_3g_ecart,
             usage_4G_TDD_ecart
             ,usage_4G_FDD_ecart,
             data_bndl_vol_ecart,
             voice_vas_cnt_ecart,
             voice_vas_amnt_ecart,
             voice_vas_main_vol_ecart,
             voice_vas_bndl_vol_ecart,
             sms_vas_cnt_ecart,
             sms_vas_bndl_cnt_ecart,
             sms_vas_amnt_ecart
             ]
    print(value)

    #Initialisation des parametres d erreur
    sms_i_cnt_error = Decimal(0)
    voice_i_cnt_error = Decimal(0)
    voice_i_vol_error = Decimal(0)
    voice_o_cnt_error = Decimal(0)
    voice_o_main_vol_error = Decimal(0)
    voice_o_bndl_vol_error = Decimal(0)
    sms_o_main_cnt_error = Decimal(0)
    sms_o_bndl_cnt_error = Decimal(0)
    data_main_vol_error = Decimal(0)
    usage_2g_error = Decimal(0)
    usage_3g_error = Decimal(0)
    usage_4G_TDD_error = Decimal(0)
    usage_4G_FDD_error = Decimal(0)
    data_bndl_vol_error = Decimal(0)
    voice_vas_cnt_error = Decimal(0)
    voice_vas_main_vol_error = Decimal(0)
    voice_vas_bndl_vol_error =Decimal(0)
    sms_vas_cnt_error =Decimal(0)
    sms_vas_bndl_cnt_error =Decimal(0)
    voice_i_amnt_error = Decimal(0)
    voice_o_amnt_error = Decimal(0)
    sms_o_amnt_error = Decimal(0)
    data_amnt_error = Decimal(0)
    voice_vas_amnt_error = Decimal(0)
    sms_vas_amnt_error = Decimal(0)


    #Calcul du pourcentage d'erreur
    if global_data['sms_i_cnt'] !=0:
        sms_i_cnt_error = (Decimal(sms_i_cnt_ecart.__str__()) /Decimal(global_data['sms_i_cnt'].__str__())) * Decimal(100)
    if global_data['voice_i_cnt'] != 0:
        voice_i_cnt_error = (Decimal(voice_i_cnt_ecart.__str__()) /Decimal(global_data['voice_i_cnt'].__str__())) * Decimal(100)
    if global_data['voice_i_vol'] != 0:
        voice_i_vol_error = (Decimal(voice_i_vol_ecart.__str__()) /Decimal(global_data['voice_i_vol'].__str__())) * Decimal(100)
    if global_data['voice_o_cnt'] != 0:
        voice_o_cnt_error = (Decimal(voice_o_cnt_ecart.__str__()) /Decimal(global_data['voice_o_cnt'].__str__())) * Decimal(100)
    if global_data['voice_o_main_vol'] != 0:
        voice_o_main_vol_error = (Decimal(voice_o_main_vol_ecart.__str__()) /Decimal(global_data['voice_o_main_vol'].__str__())) * Decimal(100)
    if global_data['voice_o_bndl_vol'] != 0:
        voice_o_bndl_vol_error = (Decimal(voice_o_bndl_vol_ecart.__str__()) /Decimal(global_data['voice_o_bndl_vol'].__str__())) * Decimal(100)
    if global_data['sms_o_main_cnt'] != 0:
        sms_o_main_cnt_error = (Decimal(sms_o_main_cnt_ecart.__str__()) /Decimal(global_data['sms_o_main_cnt'].__str__())) * Decimal(100)
    if global_data['sms_o_bndl_cnt'] != 0:
        sms_o_bndl_cnt_error = (Decimal(sms_o_bndl_cnt_ecart.__str__()) /Decimal(global_data['sms_o_bndl_cnt'].__str__())) * Decimal(100)
    if global_data['data_main_vol'] != 0:
        data_main_vol_error = (Decimal(data_main_vol_ecart.__str__()) /Decimal(global_data['data_main_vol'].__str__())) * Decimal(100)
    if global_data['usage_2G'] != 0:
        usage_2g_error = (Decimal(usage_2g_ecart.__str__()) /Decimal(global_data['usage_2G'].__str__())) * Decimal(100)
    if global_data['usage_3G'] != 0:
        usage_3g_error = (Decimal(usage_3g_ecart.__str__()) /Decimal(global_data['usage_3G'].__str__())) * Decimal(100)
    if global_data['usage_4G_TDD'] != 0:
        usage_4G_TDD_error = (Decimal(usage_4G_TDD_ecart.__str__()) /Decimal(global_data['usage_4G_TDD'].__str__())) * Decimal(100)
    if global_data['usage_4G_FDD'] !=0:
        usage_4G_FDD_error = (Decimal(usage_4G_FDD_ecart.__str__()) /Decimal(global_data['usage_4G_FDD'].__str__())) * Decimal(100)
    if global_data['data_bndl_vol'] !=0:
        data_bndl_vol_error = (Decimal(data_bndl_vol_ecart.__str__()) /Decimal(global_data['data_bndl_vol'].__str__())) * Decimal(100)
    if global_data['voice_vas_cnt'] != 0:
        voice_vas_cnt_error = (Decimal(voice_vas_cnt_ecart.__str__()) /Decimal(global_data['voice_vas_cnt'].__str__())) * Decimal(100)
    if global_data['voice_vas_main_vol'] != 0:
        voice_vas_main_vol_error = (Decimal(voice_vas_main_vol_ecart.__str__()) /Decimal(global_data['voice_vas_main_vol'].__str__())) * Decimal(100)
    if global_data['voice_vas_bndl_vol'] != 0:
        voice_vas_bndl_vol_error = (Decimal(voice_vas_bndl_vol_ecart.__str__()) /Decimal(global_data['voice_vas_bndl_vol'].__str__())) * Decimal(100)
    if global_data['sms_vas_cnt'] != 0:
        sms_vas_cnt_error = (Decimal(sms_vas_cnt_ecart.__str__()) /Decimal(global_data['sms_vas_cnt'].__str__())) * Decimal(100)
    if global_data['sms_vas_bndl_cnt'] != 0:
        sms_vas_bndl_cnt_error = (Decimal(sms_vas_bndl_cnt_error.__str__()) /Decimal(global_data['sms_vas_bndl_cnt'].__str__())) * Decimal(100)
    if global_data['voice_i_amnt'] != 0:
        voice_i_amnt_error = (Decimal(voice_i_amnt_ecart.__str__()) /Decimal(global_data['voice_i_amnt'].__str__())) * Decimal(100)
    if global_data['voice_o_amnt'] != 0:
        voice_o_amnt_error = (Decimal(voice_o_amnt_ecart.__str__()) /Decimal(global_data['voice_o_amnt'].__str__())) * Decimal(100)
    if global_data['sms_o_amnt'] != 0:
        sms_o_amnt_error = (Decimal(sms_o_amnt_ecart.__str__()) /Decimal(global_data['sms_o_amnt'].__str__())) * Decimal(100) 
    if global_data['data_amnt'] != 0:
        data_amnt_error = (Decimal(data_amnt_ecart.__str__()) /Decimal(global_data['data_amnt'].__str__())) * Decimal(100)
    if global_data['voice_vas_amnt'] != 0:
        voice_vas_amnt_error = (Decimal(voice_vas_amnt_ecart.__str__())/Decimal(global_data['voice_vas_amnt'].__str__())) * Decimal(100)
    if global_data['sms_vas_amnt'] != 0:
        sms_vas_amnt_error = (Decimal(sms_vas_amnt_ecart.__str__())/Decimal(global_data['sms_vas_amnt'].__str__())) * Decimal(100)
    value_error = [sms_i_cnt_error,
                   voice_i_cnt_error,
                   voice_i_vol_error,
                   voice_i_amnt_error,
                   voice_o_cnt_error,
                   voice_o_main_vol_error,
                   voice_o_amnt_error,
                   voice_o_bndl_vol_error,
                   sms_o_main_cnt_error,
                   sms_o_bndl_cnt_error,
                   sms_o_amnt_error,
                   data_main_vol_error,
                   data_amnt_error,
                   usage_2g_error,
                   usage_3g_error,
                   usage_4G_TDD_error,
                   usage_4G_FDD_error,
                   data_bndl_vol_error,
                   voice_vas_cnt_error,
                   voice_vas_amnt_error,
                   voice_vas_main_vol_error,
                   voice_vas_bndl_vol_error,
                   sms_vas_cnt_error,
                   sms_vas_bndl_cnt_error,
                   sms_vas_amnt_error]
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
    