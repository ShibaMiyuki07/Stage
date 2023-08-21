from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Utils import getusage_type
from database.Connexion import get_aggregation, getverification_collection
from Model.Verification import Verification
import uvicorn

origins = ['*']

app = FastAPI()

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials = True,
                   allow_methods=['*'],
                   allow_headers = ['*'])


@app.get('/bundle/{page}')
async def verification_bundle(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'bundle'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]


@app.get('/topup/{page}')
async def verification_topup(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'topup'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/om/{page}')
async def verification_om(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'om'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/usage/{page}')
async def verification_usage(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'usage'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/ec/{page}')
async def verification_ec(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'ec'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/e-rc/{page}')
async def verification_e_rc(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'e-rc'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/roaming/{page}')
async def verification_roaming(page : int):
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'roaming'}).skip(page*5).limit(5).sort('day',-1)
    return [Verification.insertion_data(r) for r in resultat]


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
    return {'data' : [Verification.insertion_data(r) for r in resultats]}

if __name__ == "__main__":
     uvicorn.run("main:app", host="0.0.0.0", port=5000, log_level="info")