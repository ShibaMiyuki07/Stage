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

if __name__ == "__main__":
    print(getall_site())