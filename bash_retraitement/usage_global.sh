#!/bin/sh
d1 = `date +'20%y-%m-%d' -d '-1 day'` 

d2 = `date +'20%y-%m-%d' -d '-1 day'` 

[[ ! -z "$1" ]] && d1 = $1 && d2 = $1
[[! -z "$2" ]] && d2 = $2

rdate = $d1

echo "rdate : $rdate"
echo "d1 : $d1"
echo "d2 : $d2"

while true 
do
    echo "Removing documents of $rdate ..."
    mongo -u "oma_dwh" -p "Dwh4@OrnZ" --authenticationDatabase "admin"<<EOF
    use cbm
    var a = new Date("$rdate");
    prinjson(a);
    printjson("remove global_daily");
    printjson(db.global_daily.remove({"day" : a,"usage_type" : {"\$in" : ["usage","bundle","topup","ec","roaming"]}}));    
EOF
    wait $!
    echo "---------------chargement $rdate-------------------" 
    php /data/script/php/mongo_usage_global_site.php $rdate $rdate 
    echo "osadmin@321" | ./sshpass.sh ssh osadmin@192.168.61.111 python /data/script/python/rattra_cbm/global_usage_site_code_name.py $rdate
    if["$rdate" == "$d2"];then break
    $rdate = $(date+%Y-%m-%d -d "$rdate + 1 day")
done