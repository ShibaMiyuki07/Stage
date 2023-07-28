def insertion_data(r):
    retour = {}
    retour['data'] = {
        'bndle_cnt' : r['bndle_cnt'],
        'bndle_amnt' : r['bndle_amnt']
    }
    return retour['data']


def calcul_error(global_data,daily_data):
    liste_key = list(daily_data.keys())
    error = []
    for i in liste_key:
        if i in daily_data and i in global_data:
            ecart =global_data[i] - daily_data[i]
            erreur = 0.0
            if global_data[i] != 0:
                erreur =(float) (ecart/global_data[liste_key[i]])*100
            if abs(erreur) >1:
                error.append([i,erreur])
    if len(error)>0:
        print(error)
        return False
    return True