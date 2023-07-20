from datetime import datetime
import sys
import mysql.connector
import pymongo

def getall_site():
    connexion = mysql.connecter.connect(user='',password='',host='127.0.0.1',database='WORK')
    cursor = connexion.cursor()
    query = "select distinct(sig_nom_site) as site_name from rf_sig_cell_krill_v3"
    cursor.execute(query)
    all_site = []
    for(site_name) in cursor:
        all_site.append(site_name)
    return all_site

def global_total_by_day_site(client,day):
    


if __name__ == "__main__":
    client = pymongo.MongoClient("")
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    print(day)
    all_site = getall_site()
