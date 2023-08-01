from datetime import datetime
import os
import sys
import pymongo
from Fonction import calcul_error, insertion_data
import mysql.connector

def getListe_Billing_type():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_billing_type"
    cursor.execute(query)
    all_billing_type = []
    for(name) in cursor:
        all_billing_type.append(name [0])
    return all_billing_type

def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$billing_type', 
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
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour


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
            '_id': '$billing_type', 
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
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_bt,client,day):
    erreur = {}
    data = []
    erreur['day'] = day
    erreur['usage_type'] = 'bundle'
    nbr_erreur = 0
    for i in range(len(liste_bt)):
        #Si billing type existe dans daily et global
        if liste_bt[i] in daily_usage and liste_bt[i] in global_daily_usage:
            daily_data = daily_usage[liste_bt[i]]
            global_data = global_daily_usage[liste_bt[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                print("Erreur de donne dans le market "+liste_bt[i].__str__())

                #Ajout des donne d'erreur dans la base de donne
                data.append({'billing_type' : liste_bt[i],"data" : error['data'],'description' : "Donne de billing type errone"})
            else:
                pass
        
        #Si billing type n'existe pas dans global daily usage
        elif liste_bt[i] in daily_usage and liste_bt[i] not in global_daily_usage:
            print(daily_usage[liste_bt[i]])
            print("Erreur de Donne de "+liste_bt[i].__str__()+" non existant dans global daily usage")

            #Ajout des donne d'erreur dans la base de donne
            data.append({'billing_type' : liste_bt[i],"data" : daily_usage[liste_bt[i]],'description' : "Donne inexistant dans global daily usage"})
        
        #Si billing type n'existe pas dans daily usage
        elif liste_bt[i] not in daily_usage and liste_bt[i] in global_daily_usage:
            print(global_daily_usage[liste_bt[i]])
            print("Erreur de Donne de "+liste_bt[i].__str__()+" non existant dans daily usage")

            #Ajout des donne d'erreur dans la base de donne
            data.append({'billing_type' : liste_bt[i],"data" : global_daily_usage[liste_bt[i]],'description' : "Donne inexistant dans daily usage"})

        #Si il n'existe pas 
        elif liste_bt[i] not in daily_usage and liste_bt[i] not in global_daily_usage:
            pass

    if nbr_erreur == 0:
        cmd = "python Verification_Segment.py "+sys.argv[1]
        os.system(cmd)
    else:
        erreur['erreur_billing_type'] = data
        erreur['erreur_billing_type_cnt'] = nbr_erreur
        insertion_donne(client,erreur)
    cmd = "python Verification_Segment.py "+sys.argv[1]
    os.system(cmd)
    

def insertion_donne(client,donne):
    db = client['test']
    collection = db['daily_usage_verification']
    resultat = collection.find({"day" : donne['day'],"usage_type" : "bundle"})
    count = 0
    for r in resultat:
        count += 1
    if count>0:
        collection.update_one({"day" : donne['day'],'usage_type' : 'bundle' },{"$set" : {"erreur_billing_type" : donne['erreur_billing_type'],"erreur_billing_type_cnt" : donne['erreur_billing_type_cnt']}})
    else:
        collection.insert_one(donne)



if __name__ == "__main__":
    liste_bt = getListe_Billing_type()
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_bt,client,day)