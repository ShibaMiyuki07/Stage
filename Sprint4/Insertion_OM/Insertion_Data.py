from datetime import datetime
import sys
import mysql.connector
import pymongo
import codecs
def getall_site():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "SELECT sig_id id, sig_code_site site, sig_nom_site nom_site, max(sig_secteur_name_v3) secteur FROM rf_sig_cell_krill_v3 sig group by sig_id  "
    cursor.execute(query)
    all_site = {}
    for(id,site,nom_site,secteur) in cursor:
          all_site[id] = {'site_name' : nom_site,'site_code' : site,'secteur' : secteur}
    print('site extracted')
    return all_site

def getmsisdn_location(day):
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_OD')
    cursor = connexion.cursor() 
    query = "select msisdn as numero,site_id  from caller_daily_location where upd_dt='"+day.__str__()+"'"
    cursor.execute(query)
    all_msisdn_location = {}
    for (numero,site_id) in cursor:
          all_msisdn_location[numero] = {'numero' : numero,'site_id' : site_id}
    print('msisdn location extracted')
    return all_msisdn_location


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client


def getom_service():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select msisdn_9,service_type,transaction_tag,classification,user_type,service from rf_om_service"
    print(query)
    cursor.execute(query)
    all_msisdn_location = {}
    for (msisdn_9,service_type,transaction_tag,classification,user_type,service) in cursor:
          msisdn = "261"+msisdn_9[1:]
          all_msisdn_location[msisdn] = {}
          all_msisdn_location[msisdn][transaction_tag] = {}
          all_msisdn_location[msisdn][transaction_tag][service_type] = {}
          all_msisdn_location[msisdn][transaction_tag][service_type]['classification'] = classification
          all_msisdn_location[msisdn][transaction_tag][service_type]['user_type'] = user_type
          all_msisdn_location[msisdn][transaction_tag][service_type]['service'] = service
    print(all_msisdn_location['261324549548']['PAIEMENT SALAIRE']['CASHIN'].keys())
    print('om service extracted')
    return all_msisdn_location


def getcollection_in_cbm(nom_collection):
    client = connexion_base()
    db = client['cbm']
    collection = db[nom_collection]
    return collection

def getsegment(day):
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
            '$match' : {
                'day' : last_month
            }
        },
        {
            '$project' : {
                '_id' : 0,
                'vbs_Segment_month' : 1,
                'party_id' : 1,
                'market' : 1,
                'billing_type' : 1,
                'pp_name' : 1
            }
        }
    ]
    retour = {}
    collection = getcollection_in_cbm('segment')
    resultats = collection.aggregate(pipeline)
    for r in resultats:
        retour[r['party_id']] = {'segment' : r['vbs_Segment_month'],'market' : r['market'],'billing_type' : r['billing_type'],'pp_name' : r['pp_name']}
    print('segment extracted')
    return retour
    

def gettransactions(day,msisdn_location,liste_segment,liste_om_service,liste_site):
    pipeline = [
          {
               '$match' : {
                    'day' : day,
                    'transaction_amount' : {"$gt" : 0},
                    'transfer_status' : "TS"
               }
          },
          {
               '$project' : {
                    '_id' : 0,
                    'sender_msisdn' : 1,
                    'receiver_msisdn_acc' : 1,
                    'transaction_amount' : 1,
                    'service_charge_received' : 1,
                    'service_type' : 1,
                    'sender_domain_code' : 1,
                    'receiver_domain' : 1,
                    'transaction_tag' : 1
               }
          }
     ]
    collection = getcollection_in_cbm('om_transactions')
    resultats = collection.aggregate(pipeline)
    data = []
    numero_sender = ""
    numero_receiver = ""
    for r in resultats:
        if r['sender_domain_code'] == 'SUBS' or r['receiver_msisdn_acc'] == 'IND01' or r['receiver_msisdn_acc'] == '261IND01' or r['receiver_msisdn_acc'] == 'PTUPS' or r['receiver_msisdn_acc'] == '261PTUPS':
            numero_sender = "261"+ r['sender_msisdn'][1:]
            numero_receiver = "261"+r['receiver_msisdn_acc'][1:]
        if r['receiver_domain'] == 'SUBS' or r['sender_msisdn'] == 'IND01' or r['sender_msisdn'] == '261IND01' or r['sender_msisdn'] == 'PTUPS' or r['sender_msisdn'] == '261PTUPS':
            numero_sender =  "261"+r['receiver_msisdn_acc'][1:]
            if r['sender_msisdn'] != '261PTUPS' or r['sender_msisdn'] != '261IND01':
                numero_receiver =  "261"+r['sender_msisdn'][1:]
            else:
                numero_receiver = r['sender_msisdn']
    
        classification = None
        service = None
        if numero_sender in liste_om_service :
          print(liste_om_service[numero_sender].keys())
          '''if r['transaction_tag'] in liste_om_service[numero_sender]:
            if r['service_type'] in liste_om_service[numero_sender][r['transaction_tag']]:
              if liste_om_service[numero_sender][r['transaction_tag']][r['service_type']]['user_type'] == 'sender':
                classification = liste_om_service[numero_sender][r['transaction_tag']][r['service_type']]['classification']
                service =  liste_om_service[numero_sender][r['transaction_tag']][r['service_type']]['service']
              else:
                  if numero_receiver in liste_om_service:
                      if r['transaction_tag'] in liste_om_service[numero_receiver]:
                        if r['service_type'] in liste_om_service[numero_receiver]:
                          if liste_om_service[numero_receiver][r['transaction_tag']][r['service_type']]['user_type'] == 'receiver':
                            classification = liste_om_service[numero_receiver][r['transaction_tag']][r['service_type']]['classification']
                            service = liste_om_service[numero_receiver][r['transaction_tag']][r['service_type']]['service']
                          else:
                            classification = r['transaction_tag']
                            service = "AUTRES"'''

        type_compte = ''
        if r['sender_domain_code'] == 'SUBS' or r['receiver_domain'] == 'SUBS':
            type_compte = 'SUBSCRIBER'
        else:
            type_compte = 'CHANNEL'
            
        site_name = None
        secteur = None
        if numero_sender in msisdn_location:
          if msisdn_location[numero_sender]['site_id'] in liste_site:
            site_name = liste_site[msisdn_location[numero_sender]['site_id']]['site_name']
            secteur = liste_site[msisdn_location[numero_sender]['site_id']]['secteur']
          
        segment = None
        market = None
        billing_type = None
        pp_name = None
        if numero_sender in liste_segment:
          segment = liste_segment[numero_sender]['segment']
          market = liste_segment[numero_sender]['market']
          billing_type = liste_segment[numero_sender]['billing_type']
          pp_name = liste_segment[numero_sender]['pp_name']
          
        
         
        data.append({
            'day' : day,
            "site_name" : site_name,
            'segment' : segment,
            'market' : market,
            'billing_type' : billing_type,
            'pp_name' : pp_name,
            'transaction_type' : r['service_type'].__str__()+"|"+ r['transaction_tag'].__str__(),
            'om_tr_amnt' : r['transaction_amount'],
            'om_amnt' : r['service_charge_received'],
            'classification' : classification,
            'service' : service,
            'type_compte' : type_compte,
            'secteur' : secteur
            })
        
    insertion_om_details(data)
    print('insertion termine')

def insertion_om_details(data):
    client = connexion_base()
    db = client['test']
    collection = db['tmp_om_details']
    collection.delete_many({})
    collection.insert_many(data)



if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_site = getall_site()
    msisdn_location = getmsisdn_location(date)
    liste_segment = getsegment(day)
    liste_om_service = getom_service()
    gettransactions(day,msisdn_location,liste_segment,liste_om_service,liste_site)
