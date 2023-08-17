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

@app.get('/bundle_details/{date}')
async def verification_bundle_details(date:str):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    resultat = collection.find({"usage_type" : 'bundle','day' : day})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/topup_details/{date}')
async def verification_topup_details(date:str):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    resultat = collection.find({"usage_type" : 'topup','day' : day})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/om_details/{date}')
async def verification_om_details(date:str):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    resultat = collection.find({"usage_type" : 'om','day' : day})
    return [Verification.insertion_data(r) for r in resultat]

@app.get('/ec_details/{date}')
async def verification_om_details(date:str):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    resultat = collection.find({"usage_type" : 'ec','day' : day})
    return [Verification.insertion_data(r) for r in resultat]


@app.get('/roaming_details/{date}')
async def verification_om_details(date:str):
    collection = getverification_collection()
    day = Verification.remplacement_date(date)
    resultat = collection.find({"usage_type" : 'roaming','day' : day})
    return [Verification.insertion_data(r) for r in resultat]