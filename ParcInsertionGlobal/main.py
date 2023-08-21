from datetime import datetime, timedelta
import sys
from Utils import calcul_date, getall_site, getcollection_daily_usage, getcollection_to_insert_parc, getsegment

def insertion_data(day,site,segment):
    collection = getcollection_daily_usage()
    resultats = collection.find({'day' : day})
    data = []
    for r in resultats:
        insertion = {}
        insertion['day'] = day
        insertion['billing_type'] = r['billing_type']
        insertion['market'] = r['market_id']
        insertion['gender'] = r['gender']
        insertion['segment'] = None
        if r['party_id'] in segment :
          insertion['segment'] = segment[r['party_id']]
        insertion['site_name'] = r['last_location_name']
        insertion['site_code'] = None
        insertion['secteur'] = None
        if r['last_location_name'] in site:
          insertion['site_code'] = site[r['last_location_name']]['code']
          insertion['secteur'] = site[r['last_location_name']]['secteur']
        
        difference_anciennete = calcul_date(day,r['first_event_date']).days
        anciennete = None
        if difference_anciennete<90:
            anciennete = "[0-3 mois]"
        elif 90<difference_anciennete and difference_anciennete<=180:
            anciennete = "]3-6 mois]"
        elif difference_anciennete<180 and difference_anciennete<=365:
            anciennete = "]6-12 mois]"
        elif difference_anciennete>365:
            anciennete = "]+12 mois]"
        insertion['anciennete'] = anciennete

        age = None
        age_actuelle =0
        if r['birthday_date'] != None:
          age_actuelle =  calcul_date(day,r['birthday_date']).days/365
        
        if 18<age_actuelle and age_actuelle<=25:
            age = "Fresh"
        elif 25<age_actuelle and age_actuelle<=45:
            age = "Master"
        elif 45<age_actuelle and age_actuelle<=60:
            age = "Senior"
        elif 60<age_actuelle:
            age = "Veteran"

        insertion['age'] = age 

        if r['orange_base_status'] == 'active' or r['orange_base_status'] == 'reactivated' or r['orange_base_status'] == 'new':
            insertion['parc_FT'] = 1
        if day == datetime(r['first_event_date'].year,r['first_event_date'].month,r['first_event_date'].day):
            insertion['activation'] = 1
        if r['orange_base_status'] == 'reactivated':
            insertion['reconnexion'] =1
        if r['orange_base_status'] == 'churn':
            insertion['deconnexion'] = 1
        if r['last_topup_date'] != None and day == datetime(r['last_topup_date'].year,r['last_topup_date'].month,r['last_topup_date'].day):
            insertion['parc_recharge_1j'] = 1
        if r['last_topup_date'] != None and calcul_date(day,r['last_topup_date']).days<30:
            insertion['parc_recharge_30j'] = 1

        day_29_j = r['day'] - timedelta(29)
        if (r['last_event_o_date'] != None and r['last_event_o_date'] <day and r['last_event_o_date']>day_29_j) or (r['last_data_date'] != None and r['last_data_date'] <day and r['last_data_date'] > day_29_j) or (r['last_om_transaction_date'] != None and r['last_om_transaction_date']<day and r['last_om_transaction_date']>day_29_j):
            insertion['charged_base'] = 1

        data.append(insertion)
        
    getcollection_to_insert_parc().insert_many(data)



if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    liste_site = getall_site()
    liste_segment = getsegment(day)
    insertion_data(day,liste_site,liste_segment)