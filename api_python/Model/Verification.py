from datetime import datetime

class Verification:
    def insertion_data(r,liste_retraitement_en_cours):
        data = {}
        key = list(r.keys())
        erreur = 0
        exec = 0
        for i in key:
            if i != "_id":
                data[i] = r[i]
                
            if i.__contains__('cnt'):
                if r[i] != 0:
                    erreur = 1
        if r['usage_type'] in liste_retraitement_en_cours and r['day'] in liste_retraitement_en_cours[r['usage_type']]:
            if liste_retraitement_en_cours[r['usage_type']][r['day']] == 1:
                exec = 1

        data['validation'] = erreur
        data['execution'] = exec
        return data
    
    def remplacement_date(date):
        date_time = datetime.strptime(date,'%Y-%m-%d')
        day = datetime(date_time.year,date_time.month,date_time.day)
        return day