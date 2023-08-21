from datetime import datetime
import sys
from Utils import getcollection_contract, getcollection_insertion, insertion_data


def Insertion_day(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$group': {
            '_id': '$day', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'day','parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_site_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$group': {
            '_id': '$last_location_name', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'site_name','site_name' : r['_id'],'parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_pp_name(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$group': {
            '_id': '$price_plan', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'pp_name','pp_name' : r['_id'],'parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_market(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$group': {
            '_id': '$market', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'market','market' : r['_id'],'parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def Insertion_billing_type(day):
    data = []
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'billing_type','billing_type' : r['_id'],'parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
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
            'day': day
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
        '$group': {
            '_id': '$segment.vbs_Segment_month', 
            'parc_FT': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$or': [
                                {
                                    '$eq': [
                                        '$orange_base_status', 'active'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'reactivated'
                                    ]
                                }, {
                                    '$eq': [
                                        '$orange_base_status', 'new'
                                    ]
                                }
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'activation': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$first_event_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'reconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'reactivated'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'deconnexion': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$eq': [
                                '$orange_base_status', 'churn'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }, 
            'parc_rec_1j': {
                '$sum': {
                    '$cond': {
                        'if': {
                            '$gte': [
                                '$last_topup_date', '$day'
                            ]
                        }, 
                        'then': 1, 
                        'else': 0
                    }
                }
            }
        }
    }
]
    
    collection = getcollection_contract()
    resultat = collection.aggregate(pipeline)
    for r in resultat:
        data.append({ 'day': day,'usage_type' : 'parc','type_aggregation' : 'segment','segment' : r['_id'],'parc_FT' : r['parc_FT'],'activation' : r['activation'],'reconnexion' : r['reconnexion'],'deconnexion' : r['deconnexion'],'parc_rec_1j' : r['parc_rec_1j'] })
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    Insertion_day(day)
    Insertion_site_name(day)
    Insertion_pp_name(day)
    Insertion_market(day)
    Insertion_billing_type(day)
    insertion_segment(day)
