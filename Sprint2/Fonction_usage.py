def insertion_data(r):
    retour = {}
    retour['data'] = {
        'bndle_cnt' : r['bndle_cnt'],
        'bndle_amnt' : r['bndle_amnt']
    }
    return retour['data']


def calcul_error(global_data,daily_data):
    liste_key = list(daily_data.keys())
    error = {}
    for i in range(liste_key):
        if liste_key[i] in daily_data and liste_key in global_data:
            ecart =global_data[liste_key[i]] - daily_data[liste_key[i]]
            erreur = 0.0
            if global_data[liste_key[i]] != 0:
                erreur =(float) (daily_data[liste_key[i]]/global_data[liste_key[i]])*100
            if erreur >abs(1):
                error[liste_key[i]] = erreur
    if error.keys() != None:
        return False
    return True