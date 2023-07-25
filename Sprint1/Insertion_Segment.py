from datetime import datetime
import pymongo
import sys
from fonction_usage import data_daily_usage

def get_all_segment(day,client):
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
  query = { "day" : last_month }
  db = client['cbm']
  collection = db['segment']
  resultat = collection.find(query,{ "party_id" : 1,"vbs_Segment_month" : 1 })
  retour = {}
  for r in resultat:
    retour[r['party_id']] = { 'segment' : r['vbs_Segment_month'] }
  print("Fin extraction segment par utilisateur")
  return retour
  
'''def get_segment_party_id(day,client,party_id):
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
    return r['vbs_Segment_month']'''
  
  
  
def get_all_data_from_daily_usage(day,client,liste_segment):
  
  print('debut extraction daily usage')
  '''pipeline = [
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
  insertion_data(inserez,client)
  return retour'''
  
  #test numero 2
  #En utilisant l'algorithme utilise dans le serveur 199
  query = { "day" : day ,"usage" : { "$exists" : True }}
  db = client['cbm']
  collection = db['daily_usage']
  resultat = collection.find(query)
  liste = []
  for r in resultat:
    inserez = {}
    inserez['segment'] = liste_segment[r['party_id']]
    inserez['day'] = day
    for i in range(len(r['usage'])):
    
      inserez['op_code'] = r['usage'][i]['op_code']
      
      ''' Si usage contient usage_op '''
      if "usage_op" in r['usage'][i]:
        for j in range(len(r['usage'][i]['usage_op'])):
        
        #Check si site name existe dans usage_op
          if "site_name" in r['usage'][i]['usage_op'][j]:
            inserez['site_name'] = r['usage'][i]['usage_op'][j]['site_name']
          else:
            inserez['site_name'] = None
            
        #Check si voice_o_cnt existe dans usage_op
          if "voice_o_cnt" in r['usage'][i]['usage_op'][j] : 
            inserez['voice_o_cnt'] = r['usage'][i]['usage_op'][j]['voice_o_cnt']
          else : 
            inserez['voice_o_cnt'] = 0
            
        #Check si voice_o_main_vol existe dans usage_op    
          if "voice_o_main_vol" in r['usage'][i]['usage_op'][j] : 
            inserez['voice_o_main_vol'] = r['usage'][i]['usage_op'][j]['voice_o_main_vol']
          else : 
            inserez['voice_o_main_vol'] = 0
            
            
        #Check si voice_o_amnt existe dans usage_op 
          if "voice_o_amnt" in r['usage'][i]['usage_op'][j] : 
            inserez['voice_o_amnt'] = r['usage'][i]['usage_op'][j]['voice_o_amnt']
          else : 
            inserez['voice_o_amnt'] = 0
            
        #Check si voice_o_bndl_vol existe dans usage_op    
          if "voice_o_bndl_vol" in r['usage'][i]['usage_op'][j] : 
            inserez['voice_o_bndl_vol'] = r['usage'][i]['usage_op'][j]['voice_o_bndl_vol']
          else : 
            inserez["voice_o_bndl_vol"] = 0
            
        #Check si sms_o_main_cnt existe dans usage_op 
          if "sms_o_main_cnt" in r['usage'][i]['usage_op'][j] : 
            inserez['sms_o_main_cnt'] = r['usage'][i]['usage_op'][j]["sms_o_main_cnt"]
          else : 
            inserez['sms_o_main_cnt'] = 0
            
        
        #Check si sms_o_bndl_cnt existe dans usage_op 
          if "sms_o_bndl_cnt" in r['usage'][i]['usage_op'][j] :
            inserez['sms_o_bndl_cnt'] = r['usage'][i]['usage_op'][j]['sms_o_bndl_cnt']
          else:
            inserez['sms_o_bndl_cnt'] = 0
            
            
        #Check si sms_o_amnt existe dans usage_op 
          if "sms_o_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['sms_o_amnt'] = r['usage'][i]['usage_op'][j]['sms_o_amnt']
          else:
            inserez["sms_o_amnt"] = 0
            
            
        #Check si data_main_vol existe dans usage_op 
          if "data_main_vol" in r['usage'][i]['usage_op'][j]:
            inserez["data_main_vol"] = r['usage'][i]['usage_op'][j]['data_main_vol']
          else:
            inserez['data_main_vol'] = 0
          
          
        #Check si data_amnt existe dans usage_op 
          if "data_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['data_amnt'] = r['usage'][i]['usage_op'][j]['data_amnt']
          else :
            inserez['data_amnt'] = 0
            
        #Check si usage_2G existe dans usage_op 
          if "usage_2G" in r['usage'][i]['usage_op'][j]:
            inserez['usage_2G'] = r['usage'][i]['usage_op'][j]['usage_2G']
          else:
            inserez['usage_2G'] = 0
        
        #Check si usage_3G existe dans usage_op 
          if "usage_3G" in r['usage'][i]['usage_op'][j]:
            inserez['usage_3G'] = r['usage'][i]['usage_op'][j]['usage_3G']
          else :
            inserez['usage_3G'] = 0
          
        #Check si usage_4G_FDD existe dans usage_op 
          if "usage_4G_FDD" in r['usage'][i]['usage_op'][j]:
            inserez['usage_4G_FDD'] = r['usage'][i]['usage_op'][j]['usage_4G_FDD']
          else:
            inserez['usage_4G_FDD'] = 0
          
          
        #Check si usage_4G_TDD existe dans usage_op 
          if "usage_4G_TDD" in r['usage'][i]['usage_op'][j]:
            inserez['usage_4G_TDD'] = r['usage'][i]['usage_op'][j]['usage_4G_TDD']
          else:
            inserez['usage_4G_TDD'] = 0
          
        #Check si data_bndl_vol existe dans usage_op 
          if "data_bndl_vol" in r['usage'][i]['usage_op'][j]:
            inserez['data_bndl_vol'] = r['usage'][i]['usage_op'][j]['data_bndl_vol']
          else:
            inserez['data_bndl_vol'] = 0
          
          
        #Check si voice_vas_cnt existe dans usage_op 
          if "voice_vas_cnt" in r['usage'][i]['usage_op'][j]:
            inserez['voice_vas_cnt'] = r['usage'][i]['usage_op'][j]['voice_vas_cnt']
          else:
             inserez['voice_vas_cnt'] = 0
          
          
        #Check si voice_vas_amnt existe dans usage_op 
          if "voice_vas_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['voice_vas_amnt'] = r['usage'][i]['usage_op'][j]['voice_vas_amnt']
          else:
            inserez['voice_vas_amnt'] = 0
          
          
        #Check si voice_vas_maint_vol existe dans usage_op 
          if "voice_vas_main_vol" in r['usage'][i]['usage_op'][j]:
            inserez['voice_vas_main_vol'] = r['usage'][i]['usage_op'][j]['voice_vas_main_vol']
          else:
            inserez['voice_vas_main_vol'] = 0
          
          
        #Check si voice_vas_bndl_vol existe dans usage_op 
          if "voice_vas_bndl_vol" in r['usage'][i]['usage_op'][j]:
            inserez['voice_vas_bndl_vol'] = r['usage'][i]['usage_op'][j]['voice_vas_bndl_vol']
          else :
            inserez['voice_vas_bndl_vol'] = 0
          
          
        #Check si sms_vas_cnt existe dans usage_op 
          if "sms_vas_cnt" in r['usage'][i]['usage_op'][j]:
            inserez['sms_vas_cnt'] = r['usage'][i]['usage_op'][j]['sms_vas_cnt']
          else:
            inserez['sms_vas_cnt'] = 0
          
          
        #Check si sms_vas_bndl_cnt existe dans usage_op 
          if "sms_vas_bndl_cnt" in r['usage'][i]['usage_op'][j]:
            inserez['sms_vas_bndl_cnt'] = r['usage'][i]['usage_op'][j]['sms_vas_bndl_cnt']
          else:
            inserez['sms_vas_bndl_cnt'] = 0
          
          
        #Check si sms_vas_amnt existe dans usage_op 
          if "sms_vas_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['sms_vas_amnt'] = r['usage'][i]['usage_op'][j]['sms_vas_amnt']
          else:
            inserez['sms_vas_amnt'] = 0  
           
           
        #Check si sms_i_cnt existe dans usage_op 
          if "sms_i_cnt" in r['usage'][i]['usage_op'][j]:
            inserez['sms_i_cnt'] = r['usage'][i]['usage_op'][j]['sms_i_cnt']
          else:
            inserez['sms_i_cnt'] = 0
          
          
        #Check si voice_i_cnt existe dans usage_op 
          if "voice_i_cnt" in r['usage'][i]['usage_op'][j]:
            inserez['voice_i_cnt'] = r['usage'][i]['usage_op'][j]['voice_i_cnt']
          else:
            inserez['voice_i_cnt'] = 0
            
            
        #Check si voice_i_vol existe dans usage_op 
          if "voice_i_vol" in r['usage'][i]['usage_op'][j]: 
            inserez['voice_i_vol'] = r['usage'][i]['usage_op'][j]['voice_i_vol']
          else:
            inserez['voice_i_vol'] = 0
          
          
        #Check si voice_i_amnt existe dans usage_op 
          if "voice_i_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['voice_i_amnt'] = r['usage'][i]['usage_op'][j]['voice_i_amnt']
          else:
            inserez['voice_i_amnt'] = 0   
            
          insertion_data(inserez,client) 
      
      ''' Si usage ne contient pas usage_op '''
      else:
        #Check si site name existe dans usage_op
          if "site_name" in r['usage'][i]:
            inserez['site_name'] = r['usage'][i]['site_name']
          else:
            inserez['site_name'] = None
            
        #Check si voice_o_cnt existe dans usage_op
          if "voice_o_cnt" in r['usage'][i] : 
            inserez['voice_o_cnt'] = r['usage'][i]['voice_o_cnt']
          else : 
            inserez['voice_o_cnt'] = 0
            
        #Check si voice_o_main_vol existe dans usage_op    
          if "voice_o_main_vol" in r['usage'][i] : 
            inserez['voice_o_main_vol'] = r['usage'][i]['voice_o_main_vol']
          else : 
            inserez['voice_o_main_vol'] = 0
            
            
        #Check si voice_o_amnt existe dans usage_op 
          if "voice_o_amnt" in r['usage'][i] : 
            inserez['voice_o_amnt'] = r['usage'][i]['voice_o_amnt']
          else : 
            inserez['voice_o_amnt'] = 0
            
        #Check si voice_o_bndl_vol existe dans usage_op    
          if "voice_o_bndl_vol" in r['usage'][i] : 
            inserez['voice_o_bndl_vol'] = r['usage'][i]['voice_o_bndl_vol']
          else : 
            inserez["voice_o_bndl_vol"] = 0
            
        #Check si sms_o_main_cnt existe dans usage_op 
          if "sms_o_main_cnt" in r['usage'][i] : 
            inserez['sms_o_main_cnt'] = r['usage'][i]["sms_o_main_cnt"]
          else : 
            inserez['sms_o_main_cnt'] = 0
            
        
        #Check si sms_o_bndl_cnt existe dans usage_op 
          if "sms_o_bndl_cnt" in r['usage'][i] :
            inserez['sms_o_bndl_cnt'] = r['usage'][i]['sms_o_bndl_cnt']
          else:
            inserez['sms_o_bndl_cnt'] = 0
            
            
        #Check si sms_o_amnt existe dans usage_op 
          if "sms_o_amnt" in r['usage'][i]:
            inserez['sms_o_amnt'] = r['usage'][i]['sms_o_amnt']
          else:
            inserez["sms_o_amnt"] = 0
            
            
        #Check si data_main_vol existe dans usage_op 
          if "data_main_vol" in r['usage'][i]:
            inserez["data_main_vol"] = r['usage'][i]['data_main_vol']
          else:
            inserez['data_main_vol'] = 0
          
          
        #Check si data_amnt existe dans usage_op 
          if "data_amnt" in r['usage'][i]:
            inserez['data_amnt'] = r['usage'][i]['data_amnt']
          else :
            inserez['data_amnt'] = 0
            
        #Check si usage_2G existe dans usage_op 
          if "usage_2G" in r['usage'][i]:
            inserez['usage_2G'] = r['usage'][i]['usage_2G']
          else:
            inserez['usage_2G'] = 0
        
        #Check si usage_3G existe dans usage_op 
          if "usage_3G" in r['usage'][i]:
            inserez['usage_3G'] = r['usage'][i]['usage_3G']
          else :
            inserez['usage_3G'] = 0
          
        #Check si usage_4G_FDD existe dans usage_op 
          if "usage_4G_FDD" in r['usage'][i]:
            inserez['usage_4G_FDD'] = r['usage'][i]['usage_4G_FDD']
          else:
            inserez['usage_4G_FDD'] = 0
          
          
        #Check si usage_4G_TDD existe dans usage_op 
          if "usage_4G_TDD" in r['usage'][i]:
            inserez['usage_4G_TDD'] = r['usage'][i]['usage_4G_TDD']
          else:
            inserez['usage_4G_TDD'] = 0
          
        #Check si data_bndl_vol existe dans usage_op 
          if "data_bndl_vol" in r['usage'][i]:
            inserez['data_bndl_vol'] = r['usage'][i]['data_bndl_vol']
          else:
            inserez['data_bndl_vol'] = 0
          
          
        #Check si voice_vas_cnt existe dans usage_op 
          if "voice_vas_cnt" in r['usage'][i]:
            inserez['voice_vas_cnt'] = r['usage'][i]['voice_vas_cnt']
          else:
             inserez['voice_vas_cnt'] = 0
          
          
        #Check si voice_vas_amnt existe dans usage_op 
          if "voice_vas_amnt" in r['usage'][i]:
            inserez['voice_vas_amnt'] = r['usage'][i]['voice_vas_amnt']
          else:
            inserez['voice_vas_amnt'] = 0
          
          
        #Check si voice_vas_maint_vol existe dans usage_op 
          if "voice_vas_main_vol" in r['usage'][i]:
            inserez['voice_vas_main_vol'] = r['usage'][i]['voice_vas_main_vol']
          else:
            inserez['voice_vas_main_vol'] = 0
          
          
        #Check si voice_vas_bndl_vol existe dans usage_op 
          if "voice_vas_bndl_vol" in r['usage'][i]:
            inserez['voice_vas_bndl_vol'] = r['usage'][i]['voice_vas_bndl_vol']
          else :
            inserez['voice_vas_bndl_vol'] = 0
          
          
        #Check si sms_vas_cnt existe dans usage_op 
          if "sms_vas_cnt" in r['usage'][i]:
            inserez['sms_vas_cnt'] = r['usage'][i]['sms_vas_cnt']
          else:
            inserez['sms_vas_cnt'] = 0
          
          
        #Check si sms_vas_bndl_cnt existe dans usage_op 
          if "sms_vas_bndl_cnt" in r['usage'][i]:
            inserez['sms_vas_bndl_cnt'] = r['usage'][i]['sms_vas_bndl_cnt']
          else:
            inserez['sms_vas_bndl_cnt'] = 0
          
          
        #Check si sms_vas_amnt existe dans usage_op 
          if "sms_vas_amnt" in r['usage'][i]:
            inserez['sms_vas_amnt'] = r['usage'][i]['sms_vas_amnt']
          else:
            inserez['sms_vas_amnt'] = 0  
           
           
        #Check si sms_i_cnt existe dans usage_op 
          if "sms_i_cnt" in r['usage'][i]:
            inserez['sms_i_cnt'] = r['usage'][i]['sms_i_cnt']
          else:
            inserez['sms_i_cnt'] = 0
          
          
        #Check si voice_i_cnt existe dans usage_op 
          if "voice_i_cnt" in r['usage'][i]:
            inserez['voice_i_cnt'] = r['usage'][i]['voice_i_cnt']
          else:
            inserez['voice_i_cnt'] = 0
            
            
        #Check si voice_i_vol existe dans usage_op 
          if "voice_i_vol" in r['usage'][i]: 
            inserez['voice_i_vol'] = r['usage'][i]['voice_i_vol']
          else:
            inserez['voice_i_vol'] = 0
          
          
        #Check si voice_i_amnt existe dans usage_op 
          if "voice_i_amnt" in r['usage'][i]['usage_op'][j]:
            inserez['voice_i_amnt'] = r['usage'][i]['voice_i_amnt']
          else:
            inserez['voice_i_amnt'] = 0   
            
          insertion_data(inserez,client) 
  
      
  
  
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
  collection_insert = db_insert['tmp_global_daily_segment']
  collection_insert.insert_one(data)
  
  
if __name__ == '__main__':
  client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
  date = sys.argv[1]
  date_time = datetime.strptime(date,'%Y-%m-%d')
  day = datetime(date_time.year,date_time.month,date_time.day)
  liste_segment = get_all_segment(day,client)
  daily_usage = get_all_data_from_daily_usage(day,client,liste_segment)
  #insertion_in_data(client,day,liste_segment,daily_usage)
  
  
  