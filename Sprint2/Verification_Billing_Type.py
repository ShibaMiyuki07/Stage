import os
import sys
from Utils import calcul_error, date_to_datetime, getcollection_daily_aggrege, insertion_data, insertion_database
import mysql.connector

def getListe_Billing_type():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_billing_type"
    cursor.execute(query)
    all_billing_type = []
    for(name) in cursor:
        all_billing_type.append(name)
    return all_billing_type

def getdaily_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day,
            'usage_type' : 'bundle',
            'type_aggregation' : "billing_type"
        }
    },
    {
        '$project' : {
            '_id' : '$billing_type',
            'bndle_cnt' : 1,
            'bndle_amnt' : 1
        }
    }
]
    collection = getcollection_daily_aggrege()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def getglobal_usage(day):
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
    collection = getcollection_daily_aggrege()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_billing_type,day):
    donne_erreur = {}
    nbr_erreur = 0
    data = []
    donne_erreur['day'] = day
    donne_erreur['usage_type'] = 'bundle'
    for i in range(len(liste_billing_type)):
        if liste_billing_type[i] in daily_usage and liste_billing_type[i] in global_daily_usage:
            global_data = global_daily_usage[liste_billing_type[i]]
            daily_data = daily_usage[liste_billing_type[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                data.append({"billing_type" : liste_billing_type[i],'error' : error['data'],'description' : 0})
            else:
                pass

        elif liste_billing_type[i] in daily_usage and liste_billing_type[i] not in global_daily_usage:
            nbr_erreur += 1
            data.append({'billing_type' : liste_billing_type[i],'error' : daily_usage[liste_billing_type[i]],'description' : -1})
        elif liste_billing_type[i] not in daily_usage and liste_billing_type[i] in global_daily_usage:
            nbr_erreur += 1
            data.append({'billing_type' : liste_billing_type[i],'error' : global_daily_usage[liste_billing_type[i]],'description' : 1})
        elif liste_billing_type[i] not in daily_usage and liste_billing_type[i] not in global_daily_usage:
            pass

    if nbr_erreur != 0:
        donne_erreur['erreur_billing_type_cnt'] = nbr_erreur
        donne_erreur['erreur_billing_type'] = data
        insertion_database(day,donne_erreur) 

    cmd = "python Verification_Bundle.py "+sys.argv[1]
    os.system(cmd)


if __name__ == "__main__":
    liste_billing_type = getListe_Billing_type()
    day = date_to_datetime(sys.argv[1])
    daily_usage = getdaily_usage(day)
    global_daily_usage = getglobal_usage(day)
    comparaison_donne(daily_usage,global_daily_usage,liste_billing_type,day)
