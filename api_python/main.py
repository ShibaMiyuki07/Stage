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