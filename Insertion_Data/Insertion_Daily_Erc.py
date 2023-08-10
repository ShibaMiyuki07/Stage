from datetime import datetime
import sys
from Utils import connexion_sql, getcollection_insertion, insertion_data

def insertion_day(day):
    query = "select count(distinct(tra_sndr_msisdn)) nb from DWH.zebra_rp2p_transaction where tra_date = '"+day+"' and (tra_sndr_category = 'RETSA' or tra_sndr_category='RETMGR') and tra_transfer_status = 200 and tra_channel = 'C2S'"
    db = connexion_sql()
    cursor = db.cursor()
    value = (day)
    cursor.execute(query)
    data = []
    for (nb) in cursor:
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'day','nb' : nb[0]})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

def insertion_site_name(day):
    query = "select sig_nom_site site_name,count(sig_nom_site) nb from WORK.caller_daily_location_ofl ms LEFT JOIN DM_RF.rf_sig_cell_krill_new rf on ms.site_id = rf.sig_id where upd_dt = '"+day+"' and msisdn IN (select distinct(concat('261',substr(tra_sndr_msisdn,2,9))) from DWH.zebra_rp2p_transaction where tra_date='"+day+"' and tra_channel='C2S' and tra_transfer_status=200 and tra_sndr_category in ('RETSA','RETMGR')) group by sig_nom_site"
    db = connexion_sql()
    cursor = db.cursor()
    cursor.execute(query)
    data = []
    for (site_name,nb) in cursor:
        data.append({ 'day' : day,'usage_type' : 'e-rc','type_aggregation' : 'site_name','site_name' : site_name,'nb' : nb})
    
    insertion_data(getcollection_insertion('tmp_daily_aggregation'),data)

if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    date_normal = day.year.__str__() + "-"+day.month.__str__()+"-"+day.day.__str__()
    insertion_day(date_normal)
    insertion_site_name(date_normal)
    