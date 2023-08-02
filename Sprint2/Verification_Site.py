import sys
from Utils import calcul_error, date_to_datetime, getcollection_daily_aggrege, getcollection_global, getdata_daily_usage, getdata_global, insertion_data, insertion_database, verification_cause
import mysql.connector

def getsite():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3 where sig_nom_site is not null"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(site_name[0])
    all_site.append("null")
    return all_site


def getdaily_usage(day):
    pipeline = [
    {
        '$match': {
            'day': day,
            'usage_type' : 'bundle',
            'type_aggregation' : "site_name"
        }
    },
    {
        '$project' : {
            '_id' : '$site_name',
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
            '_id': '$site_name', 
            'bndle_cnt': {
                '$sum': '$bndle_cnt'
            }, 
            'bndle_amnt': {
                '$sum': '$bndle_amnt'
            }
        }
    }
]
    collection = getcollection_global()
    resultat = collection.aggregate(pipeline)
    retour = {}
    for r in resultat:
        if r['_id'] != None:
            retour[r['_id']] = insertion_data(r)
        else:
            retour['null'] = insertion_data(r)
    return retour

def comparaison_donne(daily_usage,global_daily_usage,liste_site,day):
    nbr_erreur = 0
    donne_erreur = {}
    donne_erreur['day'] = day
    donne_erreur['usage_type'] = 'bundle'
    data = []
    for i in range(len(liste_site)):
        if liste_site[i] in daily_usage and liste_site[i] in global_daily_usage:
            daily_data = daily_usage[liste_site[i]]
            global_data = global_daily_usage[liste_site[i]]
            error = calcul_error(global_data,daily_data,1)
            if not error['retour']:
                nbr_erreur += 1
                data.append({'lieu' : liste_site[i],'description' : 0,'data' : error['data'],'donne errone' : verification_cause(day,liste_site[i])})
            else:
                pass
        
        elif liste_site[i] in daily_usage and liste_site[i] not in global_daily_usage:
            nbr_erreur += 1
            data.append({'lieu' : liste_site[i], 'description' : -1,'data' : daily_usage[liste_site[i]],'donne errone' : getdata_daily_usage(day,liste_site[i])})
        elif liste_site[i] not in daily_usage and liste_site[i] in global_daily_usage:
            nbr_erreur +=1
            data.append({'lieu' : liste_site[i],'description' : 1,'data' : global_daily_usage[liste_site[i]],'donne errone' : getdata_global(day,liste_site[i])})
        elif liste_site[i] not in daily_usage and liste_site[i] not in global_daily_usage:
            pass

    if nbr_erreur != 0:
        donne_erreur['erreur_site_cnt'] = nbr_erreur
        donne_erreur['erreur_site'] = data
        insertion_database(day,donne_erreur)

    
    


if __name__ == "__main__":
    liste_site = getsite()
    day = date_to_datetime(sys.argv[1])
    global_daily_usage = getglobal_usage(day)
    daily_usage = getdaily_usage(day)
    comparaison_donne(daily_usage,global_daily_usage,liste_site,day)