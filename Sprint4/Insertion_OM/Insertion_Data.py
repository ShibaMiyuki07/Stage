import datetime
import sys
import mysql.connector
def getall_site():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select sig_id_site as site_id,sig_nom_site as site_name,sig_code_site as site_code from rf_sig_cell_krill_v3  "
    cursor.execute(query)
    all_site = {}
    for(site_id,site_name,site_code) in cursor:
          all_site[site_id] = {'site_name' : site_name,'site_code' : site_code}
    return all_site

def getmsisdn_location(day):
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_OD')
    cursor = connexion.cursor() 
    query = "select msisdn as numero,site_id  from caller_daily_location where upd_dt="+day.__str__()
    cursor.execute(query)
    all_msisdn_location = {}
    for(numero,site_id) in cursor:
          all_msisdn_location[numero] = {'site_id' : site_id,'numero' : numero}
    return all_msisdn_location

if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_site = getall_site()
    print(getmsisdn_location(date))
