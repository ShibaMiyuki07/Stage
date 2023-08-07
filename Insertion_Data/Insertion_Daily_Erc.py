from datetime import datetime
import sys
from Utils import connexion_sql, getcollection_insertion, insertion_data

def getnb_semaine_annee(semaine):
    query = "select tra_date,sum(nb_msisdn) nb from WORK.final_capillarite_erec_semaine where semaine = %s group by semaine"
    db = connexion_sql()
    cursor = db.cursor()
    value= (semaine)
    cursor.execute(query,value)
    for (tra_date,nb) in cursor:
        return nb
    
def getnb_semaine_annee_site(semaine,location):
    query = "select tra_date,sum(nb_msisdn) nb from WORK.final_capillarite_erec_semaine where semaine = %s and sig_nom_site = %s"
    db = connexion_sql()
    cursor = db.cursor()
    value= (semaine,location)
    cursor.execute(query,value)
    for (tra_date,nb) in cursor:
        return nb

def insertion_day(day):
    query = "select tra_date,sum(capillarite_erecharge) nb from WORK.final_capillarite_erec_jour where tra_date = %s group by tra_date"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query,value)
    data = []
    for (tra_date,nb) in cursor:
        semaine = "S"+day.strftime("%V")+'-'+day.year.__str__()
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'day','nb' : nb,'nb_semaine' : getnb_semaine_annee(semaine)})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def insertion_site_name(day):
    query = "select sum(capillarite_erecharge) nb,sig_nom_site site_name from WORK.final_capillarite_erec_jour where tra_date = %s"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query,value)
    data = []
    for (nb,site_name) in cursor:
        semaine = "S"+day.strftime("%V")+'-'+day.year.__str__()
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'site_name','site_name' : site_name,'nb' : nb,'nb_semaine' : getnb_semaine_annee_site(semaine)})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    