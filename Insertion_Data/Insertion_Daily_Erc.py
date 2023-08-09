from datetime import datetime
import sys
from Utils import connexion_sql, getcollection_insertion, insertion_data

def insertion_day(day):
    query = "select count(distinct(tra_sndr_msisdn)) nb from DWH.zebra_rp2p_transaction where tra_date = %s and (tra_sndr_category = 'RETSA' or tra_sndr_category='RETMGR') and tra_transfer_status = 200 and tra_channel = 'C2S'"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query,value)
    data = []
    for (nb) in cursor:
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'day','nb' : nb[0]})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def insertion_site_name(day):
    query = "select sum(capillarite_erecharge) nb,sig_nom_site site_name from WORK.final_capillarite_erec_jour where tra_date = %s"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query,value)
    data = []
    for (nb,site_name) in cursor:
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'site_name','site_name' : site_name,'nb' : nb})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    