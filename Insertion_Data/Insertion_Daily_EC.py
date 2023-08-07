from datetime import datetime
import sys
from Utils import getcollection_daily_usage, getcollection_insertion, insertion_data


def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'EC': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$EC', 
            'includeArrayIndex': 'e', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$day', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'day','ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_billing_type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'EC': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$EC', 
            'includeArrayIndex': 'e', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'billing_type','billing_type' : r['_id'],'ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'EC': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$EC', 
            'includeArrayIndex': 'e', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$market', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'market','market' : r['_id'],'ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)
 
def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'EC': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$EC', 
            'includeArrayIndex': 'e', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$pp_name', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'pp_name','pp_name' : r['_id'],'ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day, 
            'EC': {
                '$exists': True
            }
        }
    }, {
        '$unwind': {
            'path': '$EC', 
            'includeArrayIndex': 'e', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$EC.site_name', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'site_name','site_name' : r['_id'],'ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
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
            'bundle': {
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
            'path': '$EC', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$segment.pot_segment_month', 
            'ec_fees': {
                '$sum': '$EC.ec_fees'
            }, 
            'ec_payback': {
                '$sum': '$EC.ec_payback'
            }, 
            'ec_loan': {
                '$sum': '$EC.ec_loan'
            }, 
            'ec_qty': {
                '$sum': '$EC.ec_qty'
            }, 
            'ca_reactivation': {
                '$sum': '$EC.ca_reactivation'
            }
        }
    }
]
    
    collection = getcollection_daily_usage()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'ec','type_aggregation' : 'segment','segment' : r['_id'],'ec_fees' : r['ec_fees'],'ec_payback':r['ec_payback'],'ec_loan':r['ec_loan'],'ec_qty':r['ec_qty'],'ca_reactivation':r['ca_reactivation'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    Insertion_day(day)
    Insertion_site_name(day)
    Insertion_market(day)
    Insertion_billing_type(day)
    Insertion_pp_name(day)
    insertion_segment(day)