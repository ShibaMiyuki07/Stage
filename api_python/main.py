from datetime import timedelta
from typing import Generator
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from Model.Utilisateur import Utilisateur
from Utils import getfichier_log, getlocation_verification, getusage_type, verification_donne,change_dict
from database.Connexion import test_login, get_aggregation, getverification_collection,getconfig
from Model.Verification import Verification
import uvicorn
import subprocess
import datetime

origins = ['*']

app = FastAPI()

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials = True,
                   allow_methods=['*'],
                   allow_headers = ['*'])

liste_retraitement_en_cours = {}
liste_retraitement_en_cours['roaming'] = {}
liste_retraitement_en_cours['usage'] = {}
liste_retraitement_en_cours['topup'] = {}
liste_retraitement_en_cours['bundle'] = {}
liste_retraitement_en_cours['e-rc'] = {}
liste_retraitement_en_cours['ec'] = {}
liste_retraitement_en_cours['nomad'] = {}
liste_retraitement_en_cours['parc'] = {}
liste_retraitement_en_cours['om'] = {}



liste_en_cours = []
liste_en_cours.append([])
liste_en_cours.append([])

'''
    Lien pour la liste
'''
@app.get('/liste/{type}/{page}')
def liste(type : int,page : int):
    print(type)
    collection = getverification_collection()
    usage_type = getusage_type(type)
    print(usage_type)
    nbr_doc = collection.count_documents({"usage_type" : usage_type})
    resultat = collection.find({"usage_type" : usage_type}).skip((page-1)*7).limit(7).sort('day',-1)
    return {'usage_type' : usage_type,'nbr_doc' : nbr_doc,'data' : [Verification.insertion_data(r,liste_retraitement_en_cours) for r in resultat]}


@app.get('/usage_type')
def get_all_usage():
    resultats = getconfig().find({'type_donne' : 'config'})
    return [change_dict(r) for r in resultats]

@app.get('/usage_type/{type}')
def get_usage_from_config(type : int):
    resultats = getconfig().find({'identifiant' : type})
    for r in resultats:
        return change_dict(r)


'''
    Lien pour les détails
'''
@app.get('/details/{date}/{type}')
def verification_details(date:str,type:int):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    resultat = collection.find({"usage_type" : usage_type,'day' : day})
    return { "usage_type" : usage_type,"day" : day,"data" : [Verification.insertion_data(r,liste_retraitement_en_cours) for r in resultat]}


'''
    Lien pour le dashboard à une date donnée
'''

@app.get('/dashboard/{type}/{date_debut}/{date_fin}')
def dashboard_bundle(type:int,date_debut : str,date_fin : str):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    date_debut = Verification.remplacement_date(date_debut)
    date_fin = Verification.remplacement_date(date_fin)
    resultat = collection.find({'usage_type' : usage_type,'day' :  { '$and ' : [{{'$gte' : date_debut},{'$lte' : date_fin}}]}}).sort('day',1)
    return [Verification.insertion_data(r) for r in resultat]


'''
    Lien pour le dashboard
'''
@app.get('/dashboard/{type}')
def dashboard_bundle(type : int):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    resultats = collection.find({'usage_type' : usage_type,'type_aggregation' : 'day'}).sort('day',-1).limit(8).sort('day',1)
    return {'usage_type' : usage_type,'data' : [Verification.insertion_data(r,liste_retraitement_en_cours) for r in resultats]}



'''
    Lien pour le retraitement à partir du tableau
'''
@app.get('/retraitement/{date}/{type}')
def retraitement(date : str,type : int):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    day = Verification.remplacement_date(date)
    resultats = collection.find({'day' : day,'usage_type' : usage_type,'type_aggregation' : 'day'})
    count = 0
    for r in resultats:
        count +=1
    if count == 0:
        return "Erreur données pas encore vérifiés"
    cmd = "sh "
    directory = "/home/osadmin/tmv/"
    a_lancer = ""
    usage_global =  ['bundle','topup','ec','usage','roaming']
    if usage_type in usage_global:
        a_lancer = "launch_global_usage.sh "
        for u in usage_global:
            liste_retraitement_en_cours[u][day] = 1
    elif(usage_type == "e-rc"):
        a_lancer = "launch_global_erc.sh "
        liste_retraitement_en_cours[usage_type][day] = 1
    else : 
        a_lancer = "launch_global_"+usage_type+".sh "
        liste_retraitement_en_cours[usage_type][day] = 1

    a_lancer = cmd+directory+a_lancer+date+" "+date
    try:
        
        commande_a_lancer = '(plink -ssh osadmin@192.168.61.199 -pw Adm3PI2 "'+a_lancer+' ") > C:\\Users\\aen_stg\\Documents\\log\\retraitement_'+getfichier_log(day,usage_type)+'_'+getfichier_log(day,usage_type)
        subprocess.run(commande_a_lancer)
        a_lancer_verification = ""
        if(usage_type in usage_global):
            for i in usage_global:
                a_lancer_verification = getlocation_verification(i,date)
                subprocess.run(a_lancer_verification,shell=True)
        else : 
            a_lancer_verification = getlocation_verification(usage_type,date)
            subprocess.run(a_lancer_verification,shell=True)
        del liste_retraitement_en_cours[usage_type][day]
        return "Retraitement terminé avec succès veuillez revoir le tableau pour voir le resultat "
    except:
        return "Erreur durant le traitement "
    
   
    
'''
    Lien pour le log du retraitement à partir du tableau
'''
@app.get('/fichier_log/{date}/{type}')
def fichier_log(date:str,type:int):
    collection = get_aggregation()
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    resultats = collection.find({'day' : day,'usage_type' : usage_type,'type_aggregation' : 'day'})
    count = 0
    for r in resultats:
        count +=1
    if count == 0:
        raise HTTPException(detail="Data not found.", status_code=status.HTTP_404_NOT_FOUND)
    fichier = "C:\\Users\\aen_stg\\Documents\\log\\retraitement_"+getfichier_log(day,usage_type)+"_"+getfichier_log(day,usage_type)
    try:
        file_contents = get_data_from_file(file_path=fichier)
        response = StreamingResponse(
            content=file_contents,
            status_code=status.HTTP_200_OK,
            media_type="text/plain",
        )
        return response
    except FileNotFoundError:
        raise HTTPException(detail="File not found.", status_code=status.HTTP_404_NOT_FOUND)


def get_data_from_file(file_path: str) -> Generator:
    with open(file=file_path, mode="rb") as file_like:
        yield file_like.read()
    
    
'''
    Lien pour la verification manuel
'''
@app.get('/verification/{date_debut}/{date_fin}/{type}')
def verification(date_debut:str,date_fin : str,type : int):
    
    day_debut = Verification.remplacement_date(date_debut)
    day_fin = Verification.remplacement_date(date_fin)
    usage_type = getusage_type(type)

    if day_fin < day_debut:
        day_inter = day_debut
        day_debut = day_fin
        day_fin = day_inter
    day_actuelle = day_debut
    if ((day_debut.day >= datetime.datetime.today().day and day_debut.month >= datetime.datetime.today().month and day_debut.year>= datetime.datetime.today().year ) 
        or (day_fin.day >= datetime.datetime.today().day and day_fin.month >= datetime.datetime.today().month and day_fin.year>= datetime.datetime.today().year )):
        return {"error" : "Les données durant cette periode ne sont pas encore initialisés"}
    
    #Vérifie que le retraitement à certaine date sont en cours
    for i in range(len(liste_en_cours[1])):
        if (day_debut == liste_en_cours[1][i]['date_debut'] or day_fin == liste_en_cours[1][i]['date_fin'] or day_fin == liste_en_cours[1][i]['date_debut'] or day_debut == liste_en_cours[1][i]['date_fin']) and type == liste_en_cours[1][i]['id_usage']:
            return {"error" : "Des données de cette période sont déjà en cours de vérification"}

    liste_en_cours[1].append({'date_debut' : day_debut,'date_fin' : day_fin,'usage_type' : usage_type,"id_usage" : type})
    
    #boucle pour lancer la vérification durant la periode donnee
    verification_donne(day_debut,day_fin,usage_type,liste_en_cours)

    for i in range(len(liste_en_cours[1])):
        if day_debut == liste_en_cours[1][i]['date_debut'] and day_fin == liste_en_cours[1][i]['date_fin'] and type == liste_en_cours[1][i]['id_usage']:
            liste_en_cours[1].pop(i)
    if day_debut != day_fin:
        return {'log' :  'Vérification de '+date_debut+" à "+date_fin+' términé'}
    else:
        return {'log' : "Vérification de "+date_debut+' términé'}


'''
    Lien pour le retraitement manuel
'''
@app.get('/retraitement_manuel/{date_debut}/{date_fin}/{type}')
def retraitement_manuel(date_debut : str,date_fin : str,type : int):
    fichier_a_lancer = ""
    usage_type = getusage_type(type)

    #Remplacement des dates en datetime
    day_debut = Verification.remplacement_date(date_debut)
    day_fin = Verification.remplacement_date(date_fin)
    if ((day_debut.day == datetime.datetime.today().day and day_debut.month == datetime.datetime.today().month and day_debut.year== datetime.datetime.today().year ) 
        or (day_fin.day == datetime.datetime.today().day and day_fin.month == datetime.datetime.today().month and day_fin.year== datetime.datetime.today().year )):
        return {"error" : "Les données durant cette periode ne sont pas encore initialisés"}
    
    #Vérifie que le retraitement à certaine date sont en cours
    for i in range(len(liste_en_cours[0])):
        if (date_debut == liste_en_cours[0][i]['date_debut'] or date_fin == liste_en_cours[0][i]['date_fin'] or date_fin == liste_en_cours[0][i]['date_debut'] or date_debut == liste_en_cours[0][i]['date_fin']) and type == liste_en_cours[0][i]['id_usage']:
            return {"error" : "Des données de cette période sont déjà retraités"}
    
    
    #Recherche des scripts shell à lancer 
    usage_global =  ['bundle','topup','ec','usage','roaming']

    #Ajoute dans la liste en cours les type d'usage en cours de traitement
    if(usage_type in usage_global):
        for i in usage_global:
            liste_en_cours[0].append({'date_debut' : day_debut,'date_fin' : day_fin,'usage_type' : i,"id_usage" : type})
    else :
        liste_en_cours[0].append({'date_debut' : day_debut,'date_fin' : day_fin,'usage_type' : usage_type,"id_usage" : type})

    if usage_type in usage_global:
        a_lancer = "launch_global_usage.sh "
    elif(usage_type == "e-rc"):
        a_lancer = "launch_global_erc.sh "
    else : 
        a_lancer = "launch_global_"+usage_type+".sh "
    fichier_a_lancer = "/home/osadmin/tmv/"+a_lancer
    cmd_retraitement = '(plink -ssh osadmin@192.168.61.199 -pw Adm3PI2 "sh '+fichier_a_lancer+date_debut+' '+date_fin+' ") > C:\\Users\\aen_stg\\Documents\\log\\retraitement_'+getfichier_log(day_debut,usage_type)+'_'+getfichier_log(day_fin,usage_type)
    subprocess.run(cmd_retraitement,shell=True)

    #Enleve les donnee retraites termines
    

    #Verifie si les donnees retraites sont les usages de depart
    if(usage_type in usage_global):
        for i in usage_global:
            liste_en_cours[1].append({'date_debut' : day_debut,'date_fin' : day_fin,'usage_type' : i,"id_usage" : type})
    else :
        liste_en_cours[1].append({'date_debut' : day_debut,'date_fin' : day_fin,'usage_type' : usage_type,"id_usage" : type})
    #Lancement de la vérification des dates retraites

    if(usage_type in usage_global):
        for usage in usage_global:       
            verification_donne(day_debut,day_fin,usage,liste_en_cours)
    else:
       verification_donne(day_debut,day_fin,usage_type,liste_en_cours)
        
    return {'log' : 'Les données de '+date_debut+" a "+date_fin+" ont été retraités et vérifiés"}


''''
    Lien pour le log du retraitement manuel
'''
@app.get('/log_retraitement/{date_debut}/{date_fin}/{type}')
def log_retraitement(date_debut : str,date_fin : str,type : int):
    day_debut = Verification.remplacement_date(date_debut)
    usage_type = getusage_type(type)
    day_fin = Verification.remplacement_date(date_fin)
    fichier_a_ouvrir = "C:\\Users\\aen_stg\\Documents\\log\\retraitement_"+getfichier_log(day_debut,usage_type)+"_"+getfichier_log(day_fin,usage_type)
    
    #Ouverture du fichier et envoi de celui-ci à l'utilisateur
    try:
        file_contents = get_data_from_file(file_path=fichier_a_ouvrir)
        response = StreamingResponse(
            content=file_contents,
            status_code=status.HTTP_200_OK,
            media_type="text/html",
        )
        return response
    except FileNotFoundError:
        raise HTTPException(detail="File not found.", status_code=status.HTTP_404_NOT_FOUND)


'''
    Lien pour la connexion de l'utilisateur
'''
@app.post('/login')
def login(user : Utilisateur):
    existe = test_login(user)
    return existe


@app.get('/en_cours/{action}')
def en_cours(action : int):
    return {'data' : liste_en_cours[action]}


@app.get('/log/{date_debut}/{date_fin}/{type}/{type_execution}')
def log_retraitement(date_debut : str,date_fin : str,type : int,type_execution : int):
    day_debut = Verification.remplacement_date(date_debut)
    usage_type = getusage_type(type)
    day_fin = Verification.remplacement_date(date_fin)
    if(type_execution == 0):
        fichier_a_ouvrir = "C:\\Users\\aen_stg\\Documents\\log\\retraitement_"+getfichier_log(day_debut,usage_type)+"_"+getfichier_log(day_fin,usage_type)
    elif(type_execution == 1):
        fichier_a_ouvrir = "log/verification"+getfichier_log(day_debut,usage_type)+"_"+getfichier_log(day_fin,usage_type)
    
    #Ouverture du fichier et envoi de celui-ci à l'utilisateur
    try:
        file_contents = get_data_from_file(file_path=fichier_a_ouvrir)
        response = StreamingResponse(
            content=file_contents,
            status_code=status.HTTP_200_OK,
            media_type="text/html",
        )
        return response
    except FileNotFoundError:
        raise HTTPException(detail="File not found.", status_code=status.HTTP_404_NOT_FOUND)


if __name__ == "__main__":
     uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")
