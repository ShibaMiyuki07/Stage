import codecs
from datetime import datetime
import os
import sys
import mysql.connector
import pymongo

from Fonction import calcul_error, insertion_data

def getsubs():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_subscriptions where name is not null"
    cursor.execute(query)
    subs_list = []
    for(name) in cursor:
        subs_list.append(codecs.encode(name[0],"UTF-8"))
    subs_list.append("null")
    return subs_list

def getdaily_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day
        }
    }, {
        '$unwind': {
            'path': '$bundle', 
            'includeArrayIndex': 'b', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$bundle.subscription', 
            'includeArrayIndex': 'b_s', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$bundle.bndle_name', 
            'bndle_cnt': {
                '$sum': '$bundle.subscription.bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bundle.subscription.bndle_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['daily_usage']
    retour = {}
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[codecs.encode(r['_id'],"UTF-8")] = insertion_data(r)
        else :
            retour['null'] = insertion_data(r)
    return retour


def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$bndle_name', 
            'bndle_cnt': {
                '$sum': '$bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    db = client['cbm']
    collection = db['global_daily_usage']
    retour = {}
    resultat = collection.aggregate(pipeline,cursor={})
    for r in resultat:
        if r['_id'] != None:
            retour[codecs.encode(r['_id'],"UTF-8")] = insertion_data(r)
        else :
            retour['null'] = insertion_data(r)
    return retour


def comparaison_donne(global_daily_usage,daily_usage,liste_subs,client,day):
    erreur = {}
    data = []
    erreur['day'] = day
    erreur['usage_type']
    nbr_erreur = 0
    for i in range(len(liste_subs)):
        #Si l'offre existe dans les deux
        if liste_subs[i] in daily_usage and liste_subs[i] in global_daily_usage:
            global_data = global_daily_usage[liste_subs[i]]
            daily_data = daily_usage[liste_subs[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                print("Erreur de donne a "+liste_subs[i].__str__())

                #Ajout des donne errone dans la base de donne
                data.append({'bndle_name' : liste_subs[i],'data' : error['data'],'description' : "Donne de bundle errone"})
            else:
                pass

        #Si l'offre inexistant dans global daily usage
        elif liste_subs[i] not in daily_usage and liste_subs[i] in global_daily_usage:
            print("Erreur de donne "+liste_subs[i].__str__()+" daily usage")

            #Ajout des donne errone dans la base de donne
            data.append({'bndle_name' : liste_subs[i],'data' : global_daily_usage[liste_subs[i]],'description' : "Donne inexistant dans daily usage"})

        #Si l'offre inexistant dans daily usage
        elif liste_subs[i] in daily_usage and liste_subs[i] not in global_daily_usage:
            print("Erreur de donne "+liste_subs[i].__str__()+" global daily usage")

            #Ajout des donne errone dans la base de donne
            data.append({'bndle_name' : liste_subs[i],'data' : daily_usage[liste_subs[i]],'description' : "Donne inexistant dans global daily usage"})

        #Si l'offre n'existe pas dans les deux
        elif liste_subs[i] not in daily_usage and liste_subs[i] not in global_daily_usage:
            pass

    #Si il n'y a eu aucune erreur
    if nbr_erreur == 0:
        cmd = "python Verification_Market.py "+sys.argv[1]
        os.system(cmd)
    else:
        erreur['erreur_bndle'] = data
        erreur['erreur_bndle_cnt'] = nbr_erreur

def insertion_donne(client,donne):
    db = client['test']
    collection = db['daily_usage_verification']
    resultat = collection.aggregate([{'$match' : {"day" : donne['day'],"usage_type" : "bundle"}},{'$count' : 'nbr'}  ])
    count = 0
    for r in resultat:
        count = r['nbr']
    if count>0:
        collection.update_one({"day" : donne['day'],'usage_type' : 'bundle' },{"$set" : {"erreur_bndle" : donne['data'],"erreur_bndle_cnt" : donne['erreur_bndle_cnt']}})
    else:
        collection.insert_one(donne)

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_subs = getsubs()
    daily_usage = getdaily_usage(client,day)
    global_daily_usage = getglobal_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_subs,client,day)