<?php

include "utils/utils_db.php";
include "utils/functions.php";
include "utils/utils_routines.php";

//DM_RF.rf_om_service
//caller_daily_location

error_reporting(E_ERROR);

$offset = 3 * 1000 * 3600;

$debut = time();

if (isset($argv[1])) {
    $d1 = $argv[1];
    $d2 = $argv[1];
}

if (isset($argv[2])) {
    if (isset($argv[3])) {
        $tranche = $argv[3];
        $d2 = $argv[2];
    } else {
        $tranche = $argv[2];
    }
}

switch ($tranche) {
    case "1":
        $nbr_filter = new MongoDB\BSON\Regex("^2613[27][1-4]");
        break;
    case "2":
        $nbr_filter = new MongoDB\BSON\Regex("^2613[27][56]");
        break;
    case "3":
        $nbr_filter = new MongoDB\BSON\Regex("^2613[27][78]");
        break;
    case "4":
        $nbr_filter = new MongoDB\BSON\Regex("^2613[27]0");
        break;
    case "5":
        $nbr_filter = new MongoDB\BSON\Regex("^2613[27]9");
        break;
}

switch ($tranche) {
    case "1":
        $sql_filter = "^2613[27][1-4]";
        break;
    case "2":
        $sql_filter = "^2613[27][56]";
        break;
    case "3":
        $sql_filter = "^2613[27][78]";
        break;
    case "4":
        $sql_filter = "^2613[27]0";
        break;
    case "5":
        $sql_filter = "^2613[27]9";
        break;
}

echo "traitement OM du " . $d1 . " au " . $d2 . "\n";

//$nbr_filter = "261328828467";

//Chargement des refs
tep_db_connect();

//Get all sites and their secteur and region
/*
// location
$location = [];
$query = tep_db_query("SELECT sig_zoneorange_name sig_zone, sig_zoneorange_name_v2 sig_zone_v2, sig_nom_site, sig_id id, sig_comment, sig_cel, sig_tech,
CONCAT(sig_lac,'-',sig_cel) lacci, sig_code_site FROM DM_RF.rf_sig_cell_krill_new sig;");
while ($res = tep_db_fetch_array($query)) {
    $location[$res["id"]]["name"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["sig_nom_site"]
    );
    $location[$res["id"]]["code"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["sig_code_site"]
    );
}
*/
$sites_id=array();
$sites = [];
$query = tep_db_query("SELECT sig_id id, sig_code_site site, sig_nom_site nom_site, max(sig_secteur_name_v3) secteur 
FROM DM_RF.rf_sig_cell_krill_v3 sig
group by sig_id;");
/*$query = tep_db_query("SELECT sig_id id, sig_code_site site, sig_nom_site nom_site, max(sig_secteur_name_v3) secteur 
FROM DM_RF.rf_sig_cell_krill_v3 sig
group by ,sig_code_site,sig_nom_site;");*/
while ($value = tep_db_fetch_array($query)) {
    $sites_id[$value["id"]]["code"]=iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $value["site"]
    );
    $sites[$value["site"]]["secteur"] = $value["secteur"];
    $sites[$value["site"]]["nom_site"] = $value["nom_site"];
}
//var_dump($sites_id);
//var_dump($sites);
//die();

$query = tep_db_query("select id, name  FROM DM_RF.rf_billing_type");
while ($res = tep_db_fetch_array($query)) {
    $btype[$res["id"]] = iconv("UTF-8", "UTF-8//IGNORE", $res["name"]);
}

$offer = [];
$query = tep_db_query("SELECT description name, tmcode, b.name billing_type
FROM DM_RF.rf_tp r left join DM_RF.rf_billing_type b on b.id = r.billing_type");
while ($res = tep_db_fetch_array($query)) {
    $offer[$res["tmcode"]]["name"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["name"]
    );
}

$custgroup = [];
$query = tep_db_query(
    "SELECT PRGCODE id, name, if(PRGCODE in (5,16), 'B2C','B2B') market FROM DM_RF.rf_customer_group"
);
while ($res = tep_db_fetch_array($query)) {
    $custgroup[$res["id"]]["name"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["name"]
    );
    $custgroup[$res["id"]]["market"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["market"]
    );
}
$user = [];
$query = tep_db_query(
    "SELECT msisdn, user_type,transaction_tag, classification,service,service_type  FROM DM_RF.rf_om_service"
);
while ($res = tep_db_fetch_array($query)) {
    $user[$res["msisdn"]][$res["transaction_tag"]][$res["service_type"]]["user_type"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["user_type"]
    );
    $user[$res["msisdn"]][$res["transaction_tag"]][$res["service_type"]]["service"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["service"]
    );
    $user[$res["msisdn"]][$res["transaction_tag"]][$res["service_type"]]["transaction_tag"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["transaction_tag"]
    );
    $user[$res["msisdn"]][$res["transaction_tag"]][$res["service_type"]]["classification"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["classification"]
    );
    $user[$res["msisdn"]][$res["transaction_tag"]][$res["service_type"]]["service_type"] = iconv(
        "UTF-8",
        "UTF-8//IGNORE",
        $res["service_type"]
    );
}

$customer = [];

$bscs_match = [
    '$match' => ["NT" => "CLT"], //, "MS" => $nbr_filter],
];
$bscs_project = [
    '$project' => [
        "MS" => 1,
        "CG" => 1,
        "BT" => 1,
        "CI" => 1,
        "CC" => 1,
        "TM" => 1,
    ],
];
$bscs_command = new MongoDB\Driver\Command([
    "aggregate" => "client",
    "pipeline" => [$bscs_match, $bscs_project],
    "cursor" => new stdClass(),
    "allowDiskUse" => true,
]);
$cursor = $manager->executeCommand("dwh", $bscs_command);
$it = new \IteratorIterator($cursor);
$it->rewind();
while ($document = $it->current()) {
    $customer[$document->MS]["billing_type"] = $btype[$document->BT];
    $customer[$document->MS]["market"] = $custgroup[$document->CG]["market"];
    $customer[$document->MS]["pp_name"] = $offer[$document->TM]["name"];
    //$customer[$document->MS]["segment"] = $segments[$document->MS]["seg"];
    $it->next();
}


$dd = $d1;
while ($dd <= $d2) {
    $mo = date("Ym", strtotime("-1 MONTH", strtotime($dd)));
    //$filter = ['day' => $day];
    $filter["day"] = date("Ym", strtotime("-1 MONTH", strtotime($dd)));
    $project = ["vbs_Segment_month" => 1, "party_id" => 1];
    //$coll = 'cbm.segment';
    $coll = "cbm.segment";
    $cmp = 0;
    $options = ["batchSize" => 1000, "timeout" => 1, "projection" => $project];
    $query = new MongoDB\Driver\Query($filter, $options);
    $cursor = $cbm_manager->executeQuery($coll, $query);
    $it = new \IteratorIterator($cursor);
    $it->rewind();
    while ($document = $it->current()) {
        $json = json_encode($document);
        $value = json_decode($json, true);
        $msisdn = $value["party_id"];
        $segments[$mo][$msisdn]["seg"] = $value["vbs_Segment_month"];
        $it->next();
    }
    echo "seg cmpt : " . $cmp . "\n";
    $dd = date("Y-m-d", strtotime($dd . " +1 month"));
}

echo "refs loaded\n";

while ($d1 <= $d2) {
    $mo = date("Ym", strtotime("-1 MONTH", strtotime($d1)));

    /*
    $newDate = date("Y-m-d", strtotime($d1 . " -1 months"));
    $prevDate = substr($newDate, 0, 4) . substr($newDate, 5, 2);

    //Get all parti_id and their segments
    $segments = [];
    $filter = ["day" => $prevDate];
    $options = [];
    $query = new \MongoDB\Driver\Query($filter, $options);
    $cursor = $cbm_manager->executeQuery("cbm.segment", $query);
    $it = new \IteratorIterator($cursor);
    $it->rewind(); // Very important

    while ($document = $it->current()) {
        $json = json_encode($document);
        $value = json_decode($json, true);
        $segments[$value["party_id"]]["seg"] = $value["vbs_Segment_month"];
        $it->next();
    }
    */

    $query = tep_db_query(
        "select msisdn, site_id from DM_OD.caller_daily_location 
		where upd_dt ='" .
            $d1 .
            "' '" .
            //"' and msisdn REGEXP '" .
            //$sql_filter .
            "'"
    );
    while ($res = tep_db_fetch_array($query)) {
        $customer[$res["msisdn"]]["site_code"] =
            $sites_id[$res["site_id"]]["code"];
    }

    //Chargement des OM

    $start = new MongoDB\BSON\UTCDateTime(strtotime($d1) * 1000 + $offset);
    $end = new MongoDB\BSON\UTCDateTime(
        strtotime($d1 . " 23:59:59") * 1000 + $offset
    );
	$dt_prev = strtotime("-1 DAY", strtotime(date("Y-m-d")));
    	$dt_prev_2 = strtotime("-2 DAY", strtotime(date("Y-m-d")));

//$coll_om = ($d == date('Y-m-d',$dt_prev)) ? 'transactions_mm_j1' :( ($d == date('Y-m-d',$dt_prev_2)) ? 'transactions_mm_j2_full' : 'transactions_mm');
   // $database = ($d1 == date('Y-m-d',$dt_prev) || $d1 == date('Y-m-d',$dt_prev_2)) ? 'dwh' : 'data_lake';
$mng = ($d1 == date('Y-m-d',$dt_prev) || $d1 == date('Y-m-d',$dt_prev_2)) ? $manager : $manager1;
    
echo $coll_om . "\n";


    $cube = [];
    $cdr_match = [
        '$match' => [
            "ED" => ['$gte' => $start, '$lte' => $end],
            "NT" => "TR",
            "AM" => ['$gt' => 0],
            //"MS" => $nbr_filter,
            "ST" => "TS",
        ],
    ];

    $cdr_command = new MongoDB\Driver\Command([
        "aggregate" => "om",
        "pipeline" => [$cdr_match],
        "cursor" => new stdClass(),
        "allowDiskUse" => true,
    ]);

  //  $cursor = $manager->executeCommand("dwh", $cdr_command);
$cursor = $mng->executeCommand("dwh", $cdr_command);

    $it = new \IteratorIterator($cursor);
    $it->rewind();
    //$i=0;
    while ($doc = $it->current()) {
        $json = json_encode($doc);
        $value = json_decode($json, true);
        $receiver = $value["RM"];
        $sender = $value["MS"];
        if (
            $user[$value["MS"]][$value["TT"]][$value["TG"]]["user_type"] == "sender"
		// &&
            //$user[$value["MS"]][$value["TT"]]["service_type"] == $value["TG"]
            //&& $user[$value["MS"]]["transaction_tag"] == $value["TT"]
        ) {
            $clas = $user[$value["MS"]][$value["TT"]][$value["TG"]]["classification"];
        } else {
            if (
                $user[$value["RM"]][$value["TT"]][$value["TG"]]["user_type"] == "receiver"/* &&
                $user[$value["RM"]][$value["TT"]]["service_type"] ==
                    $value["TG"]*/
                //&& $user[$value["RM"]]["transaction_tag"] == $value["TT"]
            ) {
                $clas = $user[$value["RM"]][$value["TT"]][$value["TG"]]["classification"];
            } else {
                $clas = $value["TT"];
            }
        }

        if (
            $user[$value["MS"]][$value["TT"]][$value["TG"]]["user_type"] == "sender"/* &&
            $user[$value["MS"]][$value["TT"]]["service_type"] == $value["TG"]*/
            //&& $user[$value["MS"]]["transaction_tag"] == $value["TT"]
        ) {
            $serv = $user[$value["MS"]][$value["TT"]][$value["TG"]]["service"];
        } else {
            if (
                $user[$value["RM"]][$value["TT"]][$value["TG"]]["user_type"] == "receiver"/* &&
                $user[$value["RM"]][$value["TT"]]["service_type"] == $value["TG"]*/
                //&& $user[$value["RM"]]["transaction_tag"] == $value["TT"]
            ) {
                $serv = $user[$value["RM"]][$value["TT"]][$value["TG"]]["service"];
            } else {
                $serv = "AUTRES";
            }
        }
        if ($value["SD"] == "SUBS" || $value["RD"] == "SUBS") {
            $type_cmpt = "SUBSCRIBER";
        } else {
            $type_cmpt = "CHANNEL";
        }

        $cube[$customer[$value["MS"]]["billing_type"]][
            $customer[$value["MS"]]["market"]
        ][$customer[$value["MS"]]["pp_name"]][
            $value["TG"] . "|" . $value["TT"]
        ][$segments[$mo][$value["MS"]]["seg"]][
            $customer[$value["MS"]]["site_code"]
        ][$clas][$serv][$type_cmpt]["om_cnt"]++;
        $cube[$customer[$value["MS"]]["billing_type"]][
            $customer[$value["MS"]]["market"]
        ][$customer[$value["MS"]]["pp_name"]][
            $value["TG"] . "|" . $value["TT"]
        ][$segments[$mo][$value["MS"]]["seg"]][
            $customer[$value["MS"]]["site_code"]
        ][$clas][$serv][$type_cmpt]["om_amnt"] += $value["SR"];
        $cube[$customer[$value["MS"]]["billing_type"]][
            $customer[$value["MS"]]["market"]
        ][$customer[$value["MS"]]["pp_name"]][
            $value["TG"] . "|" . $value["TT"]
        ][$segments[$mo][$value["MS"]]["seg"]][
            $customer[$value["MS"]]["site_code"]
        ][$clas][$serv][$type_cmpt]["om_tr_amnt"] += $value["AM"];
        $it->next();
    }
    //var_dump($cube);

    $bulk = new MongoDB\Driver\BulkWrite();
    $counter = 0;

    foreach ($cube as $billing_type => $value0) {
        foreach ($value0 as $market => $value2) {
            foreach ($value2 as $pp_name => $value3) {
                foreach ($value3 as $trans_type => $value4) {
                    foreach ($value4 as $seg => $value5) {
                        foreach ($value5 as $site_code => $value6) {
                            foreach ($value6 as $cla => $value7) {
                                foreach ($value7 as $ser => $value8) {
                                    foreach ($value8 as $type_cpt => $value9) {
                                        $x = [];
                                        $x[
                                            "day"
                                        ] = new MongoDB\BSON\UTCDateTime(
                                            strtotime($d1) * 1000 + $offset
                                        );
                                        $x["billing_type"] = $billing_type;
                                        $x["market"] = $market;
                                        $x["pp_name"] = $pp_name;
                                        $x["transaction_type"] = $trans_type;
                                        $x["segment"] = $seg;
                                        $x["site_code"] = $site_code;
                                        $x["site_name"] =
                                            $sites[$site_code]["nom_site"];
                                        $x["secteur"] =
                                            $sites[$site_code]["secteur"];
                                        $x["usage_type"] = "om";
                                        $x["classification"] = $cla;
                                        $x["service"] = $ser;
                                        $x["type_compte"] = $type_cpt;
                                        $x["om_cnt"] = $value9["om_cnt"];
                                        $x["om_amnt"] = $value9["om_amnt"];
                                        $x["om_tr_amnt"] =
                                            $value9["om_tr_amnt"];
                                        $bulk->insert($x);
                                        $counter++;

                                        if ($counter >= 5000) {
                                            $mng = $cbm_manager->executeBulkWrite(
                                                "cbm.global_daily_usage",
                                                $bulk,
                                                $writeConcern
                                            );
                                            //$mng = $cbm_manager->executeBulkWrite('cbm.global_daily_usage', $bulk, $writeConcern);
                                            echo $counter .
                                                " docs sont traités et inserés\n";
                                            $bulk = new MongoDB\Driver\BulkWrite();
                                            $counter = 0;
                                        }
                                        //var_dump($x);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    try {
        if ($counter != 0 && $bulk != null) {
            $mng = $cbm_manager->executeBulkWrite(
                "cbm.global_daily_usage",
                $bulk,
                $writeConcern
            );
            echo $counter . " docs sont traités et inserés\n";
        }
    } catch (MongoDB\Driver\Exception\Exception $e) {
        var_dump($e);
    }

    echo "End CDRs OM " .
        $d1 .
        " tranche " .
        $tranche .
        " in " .
        (time() - $debut) .
        "s\n";

    $d1 = date("Y-m-d", strtotime($d1 . " +1 days"));
}
exec("kill -9 " . getmypid());

?>
