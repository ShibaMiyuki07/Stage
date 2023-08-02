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
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$day', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','type_aggregation' : 'day','rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$topup.recharge.site_name', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'site_name' : r['_id'],'usage_type' : 'topup','type_aggregation' : 'site_name','rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_topup(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$topup.rec_type', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','rec_type' : r['_id'],'type_aggregation' : 'rec_type','rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
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
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$market', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','market' : r['_id'],'type_aggregation' : 'market','rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
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
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','type_aggregation' : 'billing_type','billing_type' : r['_id'],'rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
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
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$pp_name', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','type_aggregation' : 'pp_name','pp_name' : r['_id'],'rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
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
            'topup': {
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
            'path': '$topup', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$topup.recharge', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$segment.pot_segment_month', 
            'rec_cnt': {
                '$sum': '$topup.recharge.rec_cnt'
            }, 
            'rec_amnt': {
                '$sum': '$topup.recharge.rec_amnt'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'topup','type_aggregation' : 'segment','segment' : r['_id'],'rec_cnt' : r['rec_cnt'],'rec_amnt':r['rec_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    Insertion_day(day)
    Insertion_site_name(day)
    Insertion_topup(day)
    Insertion_Market(day)
    Insertion_Billing_Type(day)
    Insertion_pp_name(day)
    Insertion_Segment(day)