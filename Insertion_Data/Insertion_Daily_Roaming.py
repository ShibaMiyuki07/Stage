from datetime import datetime
import sys
from Utils import getcollection_daily_usage, getcollection_insertion, insertion_data

def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$day', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'day',
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_mcc(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$roaming.roaming_mcc', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'roaming_mcc',
        'roaming_mcc' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_op_code(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$roaming.op_code', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'op_code',
        'op_code' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$market', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'market',
        'market' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_billing_type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'billing_type',
        'billing_type' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$pp_name', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'pp_name',
        'pp_name' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$loc_name', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'site_name',
        'site_name' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_network_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'roaming' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$roaming.network_name', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'network_name',
        'network_name' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def insertion_segment(day):
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
            'roaming': {
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
            'path': '$roaming', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$segment.pot_segment_month', 
            'voice_o_cnt': {
                '$sum': '$roaming.voice_o_cnt'
            }, 
            'voice_o_main_vol': {
                '$sum': '$roaming.voice_o_main_vol'
            }, 
            'voice_o_amnt': {
                '$sum': '$roaming.voice_o_amnt'
            }, 
            'voice_o_bndl_vol': {
                '$sum': '$roaming.voice_o_bndl_vol'
            }, 
            'sms_o_main_cnt': {
                '$sum': '$roaming.sms_o_main_cnt'
            }, 
            'sms_o_bndl_cnt': {
                '$sum': '$roaming.sms_o_bndl_cnt'
            }, 
            'sms_o_amnt': {
                '$sum': '$roaming.sms_o_amnt'
            }, 
            'data_main_vol': {
                '$sum': '$roaming.data_main_vol'
            }, 
            'data_amnt': {
                '$sum': '$roaming.data_amnt'
            }, 
            'data_bndl_vol': {
                '$sum': '$roaming.data_bndl_vol'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        donne = { 
        'day' : day,
        'usage_type' : "roaming",
        'type_aggregation' : 'segment',
        'segment' : r['_id'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'data_bndl_vol' : r['data_bndl_vol']
        }
        data.append(donne)
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

if __name__=="__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    getcollection_insertion('tmp_daily_aggregation').delete_many({"usage_type" : "roaming",'day' : day})
    Insertion_day(day)
    Insertion_mcc(day)
    Insertion_op_code(day)
    Insertion_market(day)
    Insertion_billing_type(day)
    Insertion_pp_name(day)
    Insertion_site_name(day)
    Insertion_network_name(day)
    insertion_segment(day)