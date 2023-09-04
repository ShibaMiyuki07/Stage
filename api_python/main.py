from datetime import timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Utils import getfichier_log, getlocation_verification, getusage_type
from database.Connexion import get_aggregation, getverification_collection
from Model.Verification import Verification
import uvicorn
import os

origins = ['*']

app = FastAPI()

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials = True,
                   allow_methods=['*'],
                   allow_headers = ['*'])


@app.get('/liste/{type}/{page}')
async def liste(type : int,page : int):
    collection = getverification_collection()
    usage_type = getusage_type(type)
    nbr_doc = collection.count_documents({"usage_type" : usage_type})
    resultat = collection.find({"usage_type" : usage_type}).skip((page-1)*7).limit(7).sort('day',-1)
    return {'usage_type' : usage_type,'nbr_doc' : nbr_doc,'data' : [Verification.insertion_data(r) for r in resultat]}


@app.get('/details/{date}/{type}')
async def verification_details(date:str,type:int):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    resultat = collection.find({"usage_type" : usage_type,'day' : day})
    return { "usage_type" : usage_type,"day" : day,"data" : [Verification.insertion_data(r) for r in resultat]}


@app.get('/dashboard/{type}/{date_debut}/{date_fin}')
async def dashboard_bundle(type:int,date_debut : str,date_fin : str):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    date_debut = Verification.remplacement_date(date_debut)
    date_fin = Verification.remplacement_date(date_fin)
    resultat = collection.find({'usage_type' : usage_type,'day' :  { '$and ' : [{{'$gte' : date_debut},{'$lte' : date_fin}}]}}).sort('day',1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/dashboard/{type}')
async def dashboard_bundle(type : int):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    resultats = collection.find({'usage_type' : usage_type,'type_aggregation' : 'day'}).sort('day',-1).limit(8).sort('day',1)
    return {'usage_type' : usage_type,'data' : [Verification.insertion_data(r) for r in resultats]}


@app.get('/retraitement/{date}/{type}')
async def retraitement(date : str,type : int):
    collection = get_aggregation()
    usage_type = getusage_type(type)
    day = Verification.remplacement_date(date)
    resultats = collection.find({'day' : day,'usage_type' : usage_type,'type_aggregation' : 'day'})
    count = 0
    for r in resultats:
        count +=1
    cmd = "sh "
    directory = ""
    a_lancer = ""
    usage_global =  ['bundle','topup','ec','usage','roaming']
    if usage_type in usage_global:
        a_lancer = "usage_restant.sh "
    elif(usage_type == "e-rc"):
        a_lancer = "launch_global_erc.sh "
    else : 
        a_lancer = "launch_global_"+usage_type+".sh "

    a_lancer = cmd+directory+a_lancer+date+" "+date
    try:
        commande_a_lancer = a_lancer+" | tee retraitement_"+getfichier_log(day,usage_type)
        os.system(commande_a_lancer)
        a_lancer_verification = ""
        if(usage_type in usage_global):
            for i in usage_global:
                a_lancer_verification = getlocation_verification(i,date)
                os.system(a_lancer_verification)
        else : 
            a_lancer_verification = getlocation_verification(usage_type,date)
            os.system(a_lancer_verification)
        return {"retour" : "Retraitement terminé avec succès veuillez revoir le tableau pour voir le resultat "}
    except:
        return {"retour" : "Erreur dans le traitement "}
    
   
    

@app.get('/fichier_log/{date}/{type}')
async def fichier_log(date:str,type:int):
    collection = get_aggregation()
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    resultats = collection.find({'day' : day,'usage_type' : usage_type,'type_aggregation' : 'day'})
    count = 0
    for r in resultats:
        count +=1
    log = "Impossible de lancer le retraitement car les données n'existe pas"
    if count !=0:
        try:
            fichier ="log/verification"+usage_type+"_"+day.year.__str__()+""+day.month.__str__()+""+day.day.__str__()+".log"
            f= open(fichier)
            return {'log' :  [i for i in f]}
        except:
            return {'log' : [log]}
    
    
@app.get('/verification/{date_debut}/{date_fin}/{type}')
async def verification(date_debut:str,date_fin : str,type : int):
    day_debut = Verification.remplacement_date(date_debut)
    day_fin = day_debut
    if date_fin != None:
        day_fin = Verification.remplacement_date(date_fin)
    day_actuelle = day_debut
    while True:
        usage_type = getusage_type(type)
        directory_insertion="/Insertion_Data/"
        date = day_actuelle.strftime("%Y-%m-%d")
        cmd_insertion = "python -u "+directory_insertion+"Insertion_daily_"+usage_type+" "+date + "| tee log/verification_"+getfichier_log(day_actuelle,usage_type)
        os.system(cmd_insertion)

        directory_verification="/verification/"+usage_type+"/main.py"
        cmd_verification="python -u "+directory_verification+" "+date+" | cat log/verification_"+getfichier_log(day_actuelle,usage_type)
        os.system(cmd_verification)
        if(day_actuelle == day_fin):
            break
        day_actuelle = day_actuelle + timedelta(1)

    if day_debut != day_fin:
        return {'log' :  'verification de '+date_debut+" a "+date_fin+' termine'}
    else:
        return {'log' : "verification de "+date_debut+'termine'}


if __name__ == "__main__":
     uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")