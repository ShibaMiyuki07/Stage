from datetime import datetime
import os
import sys
import pymongo
from Fonction import insertion_data,calcul_error


def getglobal_usage(client,day):
    pipeline = [
    {
        '$match': {
            'day': day, 
            'usage_type': 'bundle'
        }
    }, {
        '$group': {
            '_id': '$market', 
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
            '_id': '$market', 
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

def comparaison_donne(daily_usage,global_daily_usage,liste_market,day,client):
    nbr_erreur = 0
    erreur = {}
    data = []
    erreur['day'] = day
    erreur['usage_type'] = 'bundle'
    for i in range(len(liste_market)):
        #Si existe dans les deux
        if liste_market[i] in daily_usage and liste_market[i] in global_daily_usage:
            daily_data = daily_usage[liste_market[i]]
            global_data = global_daily_usage[liste_market[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                print("Erreur de donne dans le market "+liste_market[i].__str__())

                #Ajout des donne pour la base de donne
                data.append({"market" : liste_market[i],'data' : error['data'],'description' : "Donne errone avec ce market"})
                
            else:
                pass

        #Si market inexistant dans global daily usage
        elif liste_market[i] in daily_usage and liste_market[i] not in global_daily_usage:
            nbr_erreur += 1
            print(daily_usage[liste_market[i]])
            print("Erreur de Donne de "+liste_market[i].__str__()+" non existant dans global daily usage")

            #Ajout des donne pour la base de donne
            data.append({"market" : liste_market[i],'data' : daily_usage[liste_market[i]],'description' : "Donne inexistante dans global daily usage"})
        
        #Si market inexistant dans daily usage
        elif liste_market[i] not in daily_usage and liste_market[i] in global_daily_usage:
            nbr_erreur += 1
            print(global_daily_usage[liste_market[i]])
            print("Erreur de Donne de "+liste_market[i].__str__()+" non existant dans daily usage")

            #Ajout des donne pour la base de donne
            data.append({"market" : liste_market[i],'data' : global_daily_usage[liste_market[i]],'description' : "Donne inexistante dans daily usage"})
        
        #Si inexistant dans les deux
        elif liste_market[i] not in daily_usage and liste_market[i] not in global_daily_usage:
            pass

    #Si il n'y a eu aucune erreur lors du traitement
    if nbr_erreur == 0:
        cmd = "python Verification_Billing_Type.py "+sys.argv[1]
        os.system(cmd)

    else:
        erreur['erreur_market_cnt'] = nbr_erreur
        erreur['erreur_market'] = data
        insertion_donne(client,erreur)
    cmd = "python Verification_Billing_Type.py "+sys.argv[1]
    os.system(cmd)

def insertion_donne(client,donne):
    db = client['test']
    collection = db['daily_usage_verification']
    resultat = collection.find({"day" : donne['day'],'usage_type' : 'bundle'})
    count = 0
    for r in resultat:
        count += 1
    if count>0:
        collection.update_one({"day" : donne['day'],'usage_type' : 'bundle' },{"$set" : {"erreur_market" : donne['data'],"erreur_market_cnt":donne['erreur_market_cnt']}})
    else:
        collection.insert_one(donne)

if __name__ == "__main__":
    liste_market = ["B2B","B2C","null"]
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    global_daily_usage = getglobal_usage(client,day)
    daily_usage = getdaily_usage(client,day)
    comparaison_donne(global_daily_usage,daily_usage,liste_market,day,client)