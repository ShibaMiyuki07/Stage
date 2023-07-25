from datetime import datetime
import pymongo
import sys
from fonction_usage import data_daily_usage

'''def get_all_segment(day,client):
  print('debut extraction segment par utilisateur')
  last_month = day.year.__str__()+day.month.__str__()
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
            'day': last_month
        }
    }
]
  db = client['cbm']
  collection = db['segment']
  resultat = collection.aggregate(pipeline,cursor={})
  retour = {}
  for r in resultat:
    retour[r['party_id']] = { 'segment' : r['vbs_Segment_month'] }
  print("Fin extraction segment par utilisateur")
  return retour
'''  
  
def get_segment_party_id(day,client,party_id):
  last_month = day.year.__str__()+day.month.__str__()
  if day.month == 1:
    date_to_use = datetime(day.year-1,12,1)
    last_month = date_to_use.year.__str__()+date_to_use.month.__str__()
  else:
    date_to_use = datetime(day.year,day.month-1,1)
    if date_to_use.month <10:
      last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
    else:
      last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
    
  db = client['cbm']
  collection = db['segment']
  pipeline = [
    {
      '$match' : {
        'day' : last_month,
        'party_id' : party_id
      }
    }
  ]
  resultat = collection.aggregate(pipeline,cursor={},allowDiskUse= True)
  for r in resultat :
    return r['vbs_Segment_month']
  
  
  
def get_all_data_from_daily_usage(day,client):
  
  print('debut extraction daily usage')
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
            '_id': '$party_id', 
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
  resultat = collection.aggregate(pipeline,cursor={}, allowDiskUse = True)
  retour = {}
  inserez = []
  i = 1
  for r in resultat:
    retour[r['_id']] = data_daily_usage(r)
    retour[r['_id']]['day'] = day
    retour[r['_id']]['segment'] =  get_segment_party_id(day,client,r['_id'])
    inserez.append(retour[r['_id']])
    print("Donne "+i.__str__()+" extracte")
    i+=1
  insertion_data(inserez,client)
  return retour
  
'''def insertion_in_data(client,day,liste_segment,daily_usage):
  print("Debut insertion des donnee")
  db = client['test']
  collection = db['tmp_daily_segment']
  list_key = list(liste_segment)
  for i in range(len(list_key)):
    if list_key[i] in daily_usage and list_key[i] in liste_segment:
      segment_data = liste_segment[list_key[i]]
      daily_data = daily_usage[list_key[i]]
      insertion = data_daily_usage(daily_data)
      insertion['segment'] = segment_data['segment']
      insertion['day'] = day
      collection.insert(insertion)
  print("insertion des donnees termine")'''
    
def insertion_data(data,client):
  db_insert = client['test']
  collection_insert = db_insert['tmp_daily_segment']
  collection_insert.insert_many(data)
  
  
if __name__ == '__main__':
  client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
  date = sys.argv[1]
  date_time = datetime.strptime(date,'%Y-%m-%d')
  day = datetime(date_time.year,date_time.month,date_time.day)
  #liste_segment = get_all_segment(day,client)
  daily_usage = get_all_data_from_daily_usage(day,client)
  #insertion_in_data(client,day,liste_segment,daily_usage)
  
  
  