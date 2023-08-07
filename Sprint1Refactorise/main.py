from datetime import datetime
import sys
from Execution_Requete import getdata_daily, getglobal_usage

from Utils import comparaison_donne, getListe_Billing_type, getall_op_code, getall_site, getliste_pp


if __name__ == "__main__":
    date = sys.argv[1]
    date_time = datetime.strptime(date,'%Y-%m-%d')
    day = datetime(date_time.year,date_time.month,date_time.day)
    donne = {}
    liste_verification = ['day','op_code','market','billing_type','pp_name','segment']
    donne['day'] = [day]
    #donne['site_name'] = getall_site()
    donne['op_code'] = getall_op_code()
    donne['market'] = ["B2B","B2C","null"]
    donne['billing_type'] = getListe_Billing_type()
    donne['pp_name'] = getliste_pp()
    donne['segment'] = ["ZERO","SUPER LOW VALUE","LOW VALUE","MEDIUM","HIGH","SUPER HIGH VALUE","NEW","RETURN","CHURN","null"]

    for i in range(len(liste_verification)):
        daily_usage = getdata_daily(day,liste_verification[i])
        global_daily_usage = getglobal_usage(day,liste_verification[i])
        comparaison_donne(global_daily_usage,daily_usage,donne[liste_verification[i]],day,liste_verification[i])
        

