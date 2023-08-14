from datetime import datetime

class Verification:
    def insertion_data(r):
        data = {}
        key = list(r.keys())
        erreur = 0
        for i in key:
            if i != "_id":
                data[i] = r[i]
                
            if i.__contains__('cnt'):
                if r[i] != 0:
                    erreur = 1
        data['validation'] = erreur 
        return data