from datetime import datetime
import sys
from Utils import getcollection_daily_usage, getcollection_insertion, insertion_data


def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'day',
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


'''def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'site_name' : r['_id'],'usage_type' : 'topup','type_aggregation' : 'site_name','rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)
'''
def Insertion_op_code(day):
    data = []
    pipeline = pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
            '_id': '$usage.op_code', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'op_code',
        'op_code' : r['_id'],
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_Market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
            '_id': '$market', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'market',
        'market' : r['_id'],
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_Billing_Type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
            '_id': '$billing_type', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'billing_type',
        'billing_type' : r['_id'],
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
            '_id': '$pp_name', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'pp_name',
        'pp_name' : r['_id'],
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_Segment(day):
    data = []
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
            'day': day, 
            'usage': {
                '$exists': True
            }
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
                        'day': last_month
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$eq': [
                                '$$party_id', '$party_id'
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
        '$unwind': {
            'path': '$usage', 
            'includeArrayIndex': 'b', 
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
            '_id': '$segment.pot_segment_month', 
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
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "usage",
        'type_aggregation' : 'segment',
        'segment' : r['_id'],
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
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
        data.append(donne)
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    Insertion_day(day)
    Insertion_Market(day)
    Insertion_Billing_Type(day)
    Insertion_pp_name(day)
    Insertion_op_code(day)
    Insertion_Segment(day)