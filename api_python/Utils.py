from datetime import timedelta
import subprocess
from Model.Verification import Verification


def getusage_type(type):
    usage_type = ""
    if type == 1:
        usage_type = "usage"
    if type == 2:
        usage_type ="bundle"
    if type == 3:
        usage_type = "topup"
    if type == 4:
        usage_type = "om"
    if type == 5:
        usage_type = "ec"
    if type == 6:
        usage_type = 'e-rc'
    if type == 7:
        usage_type = 'roaming'
    if type == 8:
        usage_type = "parc"
    if type == 9:
        usage_type = "nomad"

    return usage_type


def getfichier_log(day,usage_type):
    nom_fichier = usage_type+"_"+day.year.__str__()+""+day.month.__str__()+""+day.day.__str__()+".log"
    return nom_fichier


def getlocation_verification(usage_type,date):
    commande_python = "python -u "
    directory_verification = "/verification/"+usage_type+"/main.py "
    day = Verification.remplacement_date(date)
    a_lancer_verification = commande_python+directory_verification+date+" | tee log/verification"+getfichier_log(day,usage_type)
    return a_lancer_verification

def verification_donne(day_debut,day_fin,usage_type,en_cours):
    day_actuelle = day_debut
    while True:
                directory_insertion="/Insertion_Data/"
                date = day_actuelle.strftime("%Y-%m-%d")
                cmd_insertion = "python -u "+directory_insertion+"Insertion_daily_"+usage_type+" "+date
                subprocess.run([cmd_insertion])

                directory_verification="/verification/"+usage_type+"/main.py"
                cmd_verification="python -u "+directory_verification+" "+date
                subprocess.run([cmd_verification])

                #check si la date en cours d'execution est égale à la date finale
                if(day_actuelle == day_fin):
                    for i in range(en_cours[1]):
                        if day_debut == en_cours[1][i]['date_debut'] and day_fin == en_cours[1][i]['date_fin'] and usage_type == en_cours[1][i]['usage_type']:
                            en_cours[1].pop(i)
                    break
                day_actuelle = day_actuelle + timedelta(1)
