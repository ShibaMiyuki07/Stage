from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Utils import getusage_type
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
    resultat = collection.find({"usage_type" : usage_type}).skip((page-1)*5).limit(5).sort('day',-1)
    return {'usage_type' : usage_type,'nbr_doc' : nbr_doc,'data' : [Verification.insertion_data(r) for r in resultat]}


@app.get('/details/{date}/{type}')
async def verification_details(date:str,type:int):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    resultat = collection.find({"usage_type" : usage_type,'day' : day})
    return [Verification.insertion_data(r) for r in resultat]


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
    cmd = "python -u "
    directory = " "
    commande_a_lancer = cmd+directory+date+" | tee "+usage_type+"_"+day.year.__str__()+""+day.month.__str__()+""+day.day.__str__()+".log"
    os.system(commande_a_lancer)
    return 0
    
   
    

@app.get('/fichier_log/{date}/{type}')
async def fichier_log(date:str,type:int):
    day = Verification.remplacement_date(date)
    usage_type = getusage_type(type)
    log = "Impossible de lancer le retraitement car les données n'existe pas"
    fichier =usage_type+"_"+day.year.__str__()+""+day.month.__str__()+""+day.day.__str__()+".log"
    f= open(fichier)
    return {'log' :  [i for i in f]}


if __name__ == "__main__":
     uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")