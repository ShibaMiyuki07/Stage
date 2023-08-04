from datetime import datetime
import sys

from Utils import getcollection_for_insertion, getcollection_om_details, insertion_data



def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$day', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'om','type_aggregation' : 'day','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)

def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$site_name', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'site_name' : r['_id'],'usage_type' : 'om','type_aggregation' : 'site_name','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)

def Insertion_market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$site_name', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'market' : r['_id'],'usage_type' : 'om','type_aggregation' : 'market','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)

def Insertion_billing_type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$billing_type', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'billing_type' : r['_id'],'usage_type' : 'om','type_aggregation' : 'billing_type','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)


def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$pp_name', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'pp_name' : r['_id'],'usage_type' : 'om','type_aggregation' : 'pp_name','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)


def Insertion_Transaction_Type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$transaction_type', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'transaction_type' : r['_id'],'usage_type' : 'om','type_aggregation' : 'transaction_type','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)

def Insertion_segment(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    },  {
        '$group': {
            '_id': '$segment', 
            'om_cnt': {
                '$sum': '$om_cnt'
            }, 
            'om_tr_amnt': {
                '$sum': '$om_tr_amnt'
            },
            'om_amnt': {
                '$sum': '$om_amnt'
            },
        }
    }
]
    
    collection = getcollection_om_details()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'segment' : r['_id'],'usage_type' : 'om','type_aggregation' : 'segment','om_cnt' : r['om_cnt'],'om_tr_amnt':r['om_tr_amnt'],'om_amnt' : r['om_amnt'] })
    
    insertion_data(getcollection_for_insertion('tmp_daily_aggregation'),data)


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


    
