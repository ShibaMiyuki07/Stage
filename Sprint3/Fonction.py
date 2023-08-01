def insertion_data(r):
    data = {}
    key = list(r.keys())
    for i in key:
        if i != "_id":
            data[i] = r[i]
    return data

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