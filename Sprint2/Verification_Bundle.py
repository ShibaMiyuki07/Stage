import codecs
import mysql.connector

def getsubs():
    connexion = mysql.connector.connect(user='root',password='ShibaMiyuki07!',host='127.0.0.1',database='manitra')
    cursor = connexion.cursor() 
    query = "select name from rf_subscriptions where name is not null"
    cursor.execute(query)
    subs_list = []
    for(name) in cursor:
        subs_list.append(codecs.encode(name[0],"UTF-8"))
    subs_list.append("null")
    print("Site extracte")
    return subs_list