from datetime import datetime, timedelta
import sys
from Utils import connexion_sql, getcollection_insertion,insertion_data


def getnb_semaine(day):
    debutsemaine = date - timedelta(days=date.weekday())
    debut_semaine_string = debutsemaine.year.__str__() + "-"+debutsemaine.month.__str__()+"-"+debutsemaine.day.__str__()
    sql = "SELECT COUNT(DISTINCT(nom.identifiant_vendeur)) nb_semaine FROM `kyc_nomad` nom  LEFT JOIN `DM_RF`.`rf_Nomad` nomad ON nom.Identifiant_vendeur = nomad.Identifiant_vendeur WHERE DATE(nom.creationDate)>=CAST('[:debut_semaine]' AS DATE) AND DATE(nom.creationDate)<=CAST('[:date]' AS DATE)"
    sql = sql.replace('[:debut_semaine]',debut_semaine_string)
    sql = sql.replace('[:date]',day)
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(sql)
    data = {}
    for(nb_semaine) in cursor:
        data[day] = nb_semaine[0]
    return data


def insertion_day(day,day_normal):
    semaine = getnb_semaine(day)
    sql = "SELECT COUNT(DISTINCT(nom.identifiant_vendeur)) nb FROM `kyc_nomad` nom LEFT JOIN `DM_RF`.`rf_Nomad` rf ON rf.Identifiant_vendeur = nom.Identifiant_vendeur WHERE DATE(creationDate)=CAST('[:date]' AS DATE)"
    sql = sql.replace('[:date]',day_normal)
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(sql)
    data = []
    for (nb) in cursor:
        data.append({'day' :day,'usage_type' : 'nomad','type_aggregation' : 'day','nb' : nb[0],'nb_semaine' : semaine[day]})

    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)
    
if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    date_normal = day.year.__str__() + "-"+day.month.__str__()+"-"+day.day.__str__()
    insertion_day(day,date_normal)