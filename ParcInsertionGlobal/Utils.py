from datetime import datetime
import pymongo
import mysql.connector


def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_usage():
    client = connexion_base()
    db = client['cbm']
    collection = db['contracts']
    return collection

def getcollection_segment():
    client = connexion_base()
    db = client['cbm']
    collection = db['segment']
    return collection

def connexion_sql():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196')
    return connexion

def getall_site():
    connexion = connexion_sql()
    cursor = connexion.cursor()
    query = "select max(sig_code_site) site,sig_nom_site nom_site,sig_secteur_name_v3 secteur from DM_RF.rf_sig_cell_krill_v3 sig group by sig_nom_site"
    cursor.execute(query)
    all_site = {}
    for(site,nom_site,secteur) in cursor:
        all_site[nom_site] = {}
        all_site[nom_site]['secteur'] = secteur
        all_site[nom_site]['code'] = site
    return all_site

def getlastmonth(day):
    last_month = ""
    if day.month == 1:
        date_to_use = datetime(day.year-1,12,1)
        last_month = date_to_use.year.__str__()+date_to_use.month.__str__()
    else:
        date_to_use = datetime(day.year,day.month-1,1)
        if date_to_use.month <10:
            last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
        else:
            last_month = date_to_use.year.__str__()+"0"+date_to_use.month.__str__()
    return last_month


def getsegment(day):
    last_month = getlastmonth(day)
    collection = getcollection_daily_usage()
    resultats = collection.find({'day' : last_month})
    retour = {}
    for r in resultats : 
        retour[r['party_id']] = r['vbs_Segment_month']
    return retour

def calcul_date(debut,fin):
    difference = debut - fin
    return difference

def getcollection_to_insert_parc():
    client = connexion_base()
    db = client['test']
    collection = db['parc_details']
    return collection