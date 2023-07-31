import codecs
import mysql.connector

def getsubs():
    connexion = mysql.connector.connect(user='ETL_USER',password='3tl_4ser',host='192.168.61.196',database='DM_RF')
    cursor = connexion.cursor() 
    query = "select name from rf_subscriptions where name is not null"
    cursor.execute(query)
    subs_list = []
    for(name) in cursor:
        subs_list.append(codecs.encode(name[0],"UTF-8"))
    subs_list.append("null")
    return subs_list