from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database.Connexion import getverification_collection
from Model.Verification import Verification

origins = ['*']

app = FastAPI()

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials = True,
                   allow_methods=['*'],
                   allow_headers = ['*'])


@app.get('/bundle')
async def verification_bundle():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'bundle'})
    return [Verification.insertion_data(r) for r in resultat]


@app.get('/topup')
async def verification_topup():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'topup'})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/om')
async def verification_om():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'om'})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/usage')
async def verification_usage():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'usage'})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/ec')
async def verification_ec():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'ec'})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/e-rc')
async def verification_e_rc():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'e-rc'})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/roaming')
async def verification_roaming():
    collection = getverification_collection()
    resultat = collection.find({"usage_type" : 'roaming'})
    return [Verification.insertion_data(r) for r in resultat]


@app.get('/details/{date}/{type}')
async def verification_details(date:str,type:int):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    usage_type = None
    if type == 1:
        usage_type = "usage"
    if type == 2:
        usage_type ="bundle"
    if type == 3:
        usage_type = "topup"
    if type == 4:
        usage_type = "om"
    if type == 5:
        usage_type = "ec"
    if type == 6:
        usage_type = 'e-rc'
    if type == 7:
        usage_type = 'roaming'
    resultat = collection.find({"usage_type" : usage_type,'day' : day})
    return [Verification.insertion_data(r) for r in resultat]