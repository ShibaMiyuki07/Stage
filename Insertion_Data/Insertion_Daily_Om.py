from datetime import datetime
import sys
from Utils import getcollection_daily_usage, getcollection_insertion, insertion_data


def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day,
            'om' : {'$exists' : True}
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$day', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            },
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'om','type_aggregation' : 'day','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'om': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'o', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'ot', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$om.transaction.site_name', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'site_name' : r['_id'],'usage_type' : 'om','type_aggregation' : 'site_name','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'om': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'o', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'ot', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$market', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'market' : r['_id'],'usage_type' : 'om','type_aggregation' : 'market','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_billing_type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'om': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'o', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'ot', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'billing_type' : r['_id'],'usage_type' : 'om','type_aggregation' : 'billing_type','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'om': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'o', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'ot', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$pp_name', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'pp_name' : r['_id'],'usage_type' : 'om','type_aggregation' : 'pp_name','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


def Insertion_Transaction_Type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'om': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$om', 
            'includeArrayIndex': 'o', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'ot', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$om.transaction_type', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'transaction_type' : r['_id'],'usage_type' : 'om','type_aggregation' : 'transaction_type','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_segment(day):
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
            'om': {
                '$exists': True
            },
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
            'path': '$om', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$om.transaction', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$segment.vbs_Segment_month', 
            'om_cnt': {
                '$sum': '$om.transaction.om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om.transaction.om_tr_amnt'
            }, 
            'om_amnt': {
                '$sum': '$om.transaction.om_amnt'
            }
        }
    }, {
        '$match': {
            'om_tr_amnt': {
                '$gt': 0
            }
        }
    }
]
    
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'segment' : r['_id'],'usage_type' : 'om','type_aggregation' : 'segment','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    Insertion_day(day)
    Insertion_billing_type(day)
    Insertion_pp_name(day)
    Insertion_Transaction_Type(day)
    Insertion_market(day)
    Insertion_site_name(day)
    Insertion_segment(day)


    
