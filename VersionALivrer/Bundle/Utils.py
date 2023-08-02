import mysql.connector
import pymongo

def insertion_data(r):
    data = {}
    key = list(r.keys())
    for i in key:
        if i != "_id":
            data[i] = r[i]
    return data

def connexion_base():
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    return client

def getcollection_daily_aggrege():
    client = connexion_base()
    db = client['test']
    collection = db['tmp_daily_aggregation']
    return collection

def getcollection_global():
    client =connexion_base()
    db = client['cbm']
    collection = db['global_daily_usage']
    return collection


def comparaison_donne(global_daily_usage,daily_usage,liste,day,nom):
    erreur = {}
    nbr_erreur = 0
    erreur['day'] = day
    erreur['usage_type'] = 'bundle'
    data = []
    for i in range(len(liste)):
        if liste[i] in global_daily_usage and liste[i] in daily_usage:
            global_data = global_daily_usage[liste[i]]
            daily_data = daily_usage[liste[i]]
            error = calcul_error(global_data,daily_data,0)
            if not error['retour']:
                nbr_erreur +=1
                data.append({ nom : liste[i],'data' : error['data'],'description' : 0 })
            else:
                pass
        elif liste[i] in global_daily_usage and liste[i] not in daily_usage:
            nbr_erreur += 1
            data.append({ nom : liste[i],'data' : global_daily_usage[liste[i]],'description' : -1 })
        elif liste[i] in daily_usage and liste[i] not in global_daily_usage:
            nbr_erreur += 1
            data.append({ nom : liste[i],'data' : daily_usage[liste[i]],'description' : 1 })
        elif liste[i] not in daily_usage and liste[i] not in global_daily_usage:
            pass

    if nbr_erreur>0:
        erreur['erreur_'+nom+'_cnt'] = nbr_erreur
        erreur['erreur_'+nom] = data
        insertion_database(day,erreur)

def getcollection_for_insertion():
    client = connexion_base()
    db = client['test']
    collection = db['daily_usage_verification']
    return collection


def insertion_database(day,donne):
    collection = getcollection_for_insertion()
    resultat = collection.find({'day' : day,'usage_type' : 'bundle'})
    count = 0
    for r in resultat:
        count += 1
    
    if count>0:
        list_key = list(donne.keys())
        for r in list_key:
            collection.update_one({'day' : day,'usage_type' : 'bundle'},{"$set" : {r : donne[r]}})
    else:
        collection.insert_one(donne)


def calcul_error(global_data,daily_data,taux_erreur):
    liste_key = list(daily_data.keys())
    liste_error = []
    
    for i in liste_key:
        if i in daily_data and i in global_data:
            ecart =global_data[i] - daily_data[i]
            erreur = 0.0
            if global_data[i] != 0:
                erreur =(float) (ecart/global_data[i])*100
            if abs(erreur) >taux_erreur:
                error = {}
                error["nom"] = i
                error["error"] = erreur
                error['ecart'] = ecart
                liste_error.append(error)
    if len(liste_error)>0:
        return {"retour" : False,"data" : liste_error}
    return {"retour" : True}

def getall_site():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3 where sig_nom_site is not null"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(site_name[0])
    all_site.append("null")
    return all_site

def getall_subscription():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select distinct(name) as name from rf_subscriptions where name is not null"
    cursor.execute(query)
    all_site = []
    for(name) in cursor:
        all_site.append(name[0])
    all_site.append("null")
    return all_site

def getListe_Billing_type():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_billing_type"
    cursor.execute(query)
    all_billing_type = []
    for(name) in cursor:
        all_billing_type.append(name[0])
    all_billing_type.append('null')
    return all_billing_type

def getliste_pp():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select description from rf_tp"
    cursor.execute(query)
    liste_pp = []
    for(description) in cursor:
        liste_pp.append(description[0])
    liste_pp.append('null')
    return liste_pp