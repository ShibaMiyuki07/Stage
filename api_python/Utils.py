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
    commande_python = 'plink -ssh osadmin@192.168.61.111 -pw osadmin@321 "source /data/temp/venv_test/bin/activate ;python -u '
    directory_verification = "/verification/"+usage_type+"/main.py "
    day = Verification.remplacement_date(date)
    a_lancer_verification = commande_python+directory_verification+date+' ;dactivate"'
    return a_lancer_verification

def verification_donne(day_debut,day_fin,usage_type,en_cours):
    day_actuelle = day_debut
    while True:
        if(usage_type != 'om'):    
            directory_insertion="/data2/tmp/Stage/Insertion_Data/"
            date = day_actuelle.strftime("%Y-%m-%d")
            cmd_insertion = 'plink -ssh osadmin@192.168.61.111 -pw osadmin@321 "source /data/temp/venv_test/bin/activate ;python -u '+directory_insertion+'Insertion_Daily_'+usage_type+'.py '+date+' ;deactivate"'
            subprocess.run(cmd_insertion,shell=True)

            directory_verification="/data2/tmp/Stage/dossier_final/"+usage_type+"/main.py"
            cmd_verification='plink -ssh osadmin@192.168.61.111 -pw osadmin@321 "source /data/temp/venv_test/bin/activate ;python -u '+directory_verification+' '+date+' ;deactivate"'
            subprocess.run(cmd_verification)
            if(day_actuelle == day_fin):
                for i in range(len(en_cours[1])):
                        if day_debut == en_cours[1][i]['date_debut'] and day_fin == en_cours[1][i]['date_fin'] and usage_type == en_cours[1][i]['usage_type']:
                            en_cours[1].pop(i)
                break
            day_actuelle = day_actuelle + timedelta(1)
        else:   
            directory_extraction="/data2/tmp/Stage/dossier_final/om/Insertion_data/Extraction_Data.py"
            date = day_actuelle.strftime("%Y-%m-%d")
            cmd_insertion = 'plink -ssh osadmin@192.168.61.111 -pw osadmin@321 "source /data/temp/venv_test/bin/activate ; python -u [directory] '+date+'; deactivate"'
            to_extract = cmd_insertion.replace('[directory]',directory_extraction)
            subprocess.run(to_extract,shell=True)

            directory_extraction="/data2/tmp/Stage/dossier_final/om/Insertion_data/Insertion_Data.py"
            to_insert = cmd_insertion.replace('[directory]',directory_extraction)
            subprocess.run(to_insert,shell=True)


            directory_verification="/data2/tmp/Stage/dossier_final/"+usage_type+"/main.py"
            cmd_verification='plink -ssh osadmin@192.168.61.111 -pw osadmin@321 "source /data/temp/venv_test/bin/activate ;python -u '+directory_verification+' '+date+';deactivate"'
            print(cmd_verification)
            subprocess.run(cmd_verification,shell=True)
            if(day_actuelle == day_fin):
                for i in range(len(en_cours[1])):
                        if day_debut == en_cours[1][i]['date_debut'] and day_fin == en_cours[1][i]['date_fin'] and usage_type == en_cours[1][i]['usage_type']:
                            en_cours[1].pop(i)
                break
            day_actuelle = day_actuelle + timedelta(1)
