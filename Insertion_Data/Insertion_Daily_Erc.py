from datetime import datetime,timedelta
import sys
from Utils import connexion_sql, getcollection_insertion, insertion_data

def insertion_day(day,date):
    nb_mois = getnb_mois(day,date)
    nb_semaine = getnb_semaine(day,date)
    query = "select count(distinct(tra_sndr_msisdn)) nb from DWH.zebra_rp2p_transaction where tra_date = '"+day+"' and (tra_sndr_category = 'RETSA' or tra_sndr_category='RETMGR') and tra_transfer_status = 200 and tra_channel = 'C2S'"
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(query)
    data = []
    for (nb) in cursor:
        data.append({ 'day' : date,'usage_type' : 'e-rc','type_aggregation' : 'day','nb' : nb[0],'nb_semaine' : nb_semaine[day],'nb_mois':nb_mois[day]})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)
    
    
    
def getnb_semaine(day,date):
    debutsemaine = date - timedelta(days=date.weekday())
    debut_semaine_string = debutsemaine.year.__str__() + "-"+debutsemaine.month.__str__()+"-"+debutsemaine.day.__str__()
    query = "SELECT COUNT(DISTINCT(tra_sndr_msisdn)) nb_semaine FROM DWH.zebra_rp2p_transaction WHERE (tra_date BETWEEN  CAST('"+debut_semaine_string+"' AS DATE) AND CAST('"+day+"' AS DATE)) AND (tra_sndr_category = 'RETSA' OR tra_sndr_category = 'RETMGR' ) AND  tra_transfer_status = 200 AND tra_channel = 'C2S' "
    retour = {}
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(query)
    for(nb_semaine) in cursor:
      retour[day] = nb_semaine[0]
    return retour
    
    
def getnb_mois(day,date):
    debut_semaine_string = date.year.__str__() + "-"+date.month.__str__()+"-01"
    query = "SELECT COUNT(DISTINCT(tra_sndr_msisdn)) nb_semaine FROM DWH.zebra_rp2p_transaction WHERE (tra_date BETWEEN  CAST('"+debut_semaine_string+"' AS DATE) AND CAST('"+day+"' AS DATE)) AND (tra_sndr_category = 'RETSA' OR tra_sndr_category = 'RETMGR' ) AND  tra_transfer_status = 200 AND tra_channel = 'C2S' "
    retour = {}
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(query)
    for(nb_semaine) in cursor:
      retour[day] = nb_semaine[0]
    return retour
    
    
    
def getnb_site_semaine(day,date):
    debutsemaine = date - timedelta(days=date.weekday())
    debut_semaine_string = debutsemaine.year.__str__() + "-"+debutsemaine.month.__str__()+"-"+debutsemaine.day.__str__()
    query = "SELECT sig_nom_site site_name,count(sig_nom_site) nb FROM WORK.caller_daily_location_ofl ms LEFT JOIN DM_RF.rf_sig_cell_krill_new rf ON ms.site_id = rf.sig_id WHERE upd_dt='"+day+"'  AND  msisdn IN (SELECT DISTINCT(CONCAT('261',SUBSTR(tra_sndr_msisdn,2,9))) FROM DWH.zebra_rp2p_transaction WHERE (tra_date BETWEEN  CAST('"+debut_semaine_string+"' AS DATE) AND CAST('"+day+"' AS DATE)) AND (tra_sndr_category = 'RETSA' OR tra_sndr_category = 'RETMGR' ) AND  tra_transfer_status = 200 AND tra_channel = 'C2S' )  GROUP BY sig_nom_site"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query)
    data = {}
    for (site_name,nb) in cursor:
        data[site_name] = nb
    return data
    
    
def getnb_site_mois(day,date):
    debut_semaine_string = date.year.__str__() + "-"+date.month.__str__()+"-01"
    query = "SELECT sig_nom_site site_name,count(sig_nom_site) nb FROM WORK.caller_daily_location_ofl ms LEFT JOIN DM_RF.rf_sig_cell_krill_new rf ON ms.site_id = rf.sig_id WHERE upd_dt='"+day+"'  AND  msisdn IN (SELECT DISTINCT(CONCAT('261',SUBSTR(tra_sndr_msisdn,2,9))) FROM DWH.zebra_rp2p_transaction WHERE (tra_date BETWEEN  CAST('"+debut_semaine_string+"' AS DATE) AND CAST('"+day+"' AS DATE)) AND (tra_sndr_category = 'RETSA' OR tra_sndr_category = 'RETMGR' ) AND  tra_transfer_status = 200 AND tra_channel = 'C2S' )  GROUP BY sig_nom_site"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query)
    data = {}
    for (site_name,nb) in cursor:
        data[site_name] = nb
    return data
      

def insertion_site_name(day,date):
    nb_mois = getnb_site_mois(day,date)
    nb_semaine = getnb_site_semaine(day,date)
    query = "select sig_nom_site site_name,count(sig_nom_site) as nb from WORK.caller_daily_location_ofl ms LEFT JOIN DM_RF.rf_sig_cell_krill_new rf on ms.site_id = rf.sig_id where upd_dt = '"+day+"' and msisdn IN (select distinct(concat('261',substr(tra_sndr_msisdn,2,9))) from DWH.zebra_rp2p_transaction where tra_date='"+day+"' and tra_channel='C2S' and tra_transfer_status=200 and tra_sndr_category in ('RETSA','RETMGR')) group by sig_nom_site"
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(query)
    data = []
    for (site_name,nb) in cursor:
        data.append({ 'day' : date,'usage_type' : 'e-rc','type_aggregation' : 'site_name','site_name' : site_name,'nb' : nb,'nb_semaine' : nb_semaine[site_name],'nb_mois' : nb_mois[site_name]})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    date_normal = day.year.__str__() + "-"+day.month.__str__()+"-"+day.day.__str__()
    getcollection_insertion('tmp_daily_aggregation').delete_many({"usage_type" : "e-rc",'day' : day})
    insertion_day(date_normal,day)
    insertion_site_name(date_normal,day)
    