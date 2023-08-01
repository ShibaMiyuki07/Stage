<?php
error_reporting(E_ERROR);
include ("utils/date.php");
include ("utils/utils_db.php");
include ("utils/functions.php");
include ('utils/utils_mongodb.php');
//$manager = new MongoDB\Driver\Manager("mongodb://localhost:27017");
//$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 100);
echo "Connection to database successfully\n";
$offset = 3*3600*1000;
if($argv[1] != null){
    $d1 = $argv[1];
    $d2 = $argv[1];
}
if($argv[1] != null && $argv[2] != null){
    $d2 = $argv[2];
}
$yd = substr($d1,0,4);
$yf = substr($d2,0,4);
$md = substr($d1,5,2);
$mf = substr($d2,5,2);
$jd = substr($d1,8,2);
$jf = substr($d2,8,2);
$df = $d2 . ' 23:59:59';
//main
$d0 = time();
tep_db_connect();
echo "load referentiel \n";
// load referentiel
$type_event[1] = 'voice';
$type_event[2] = 'sms';
$type_event[3] = 'data';
// country
$country = array();
$query = tep_db_query("select calling_code, name from DM_RF.rf_country");
while ($res = tep_db_fetch_array($query)) $country[$res['calling_code']]=iconv('UTF-8', 'UTF-8//IGNORE', $res['name']); 
$mcc_country = array();
$query = tep_db_query("SELECT concat(mcc,mnc) mcc, network, name
FROM DM_RF.rf_mcc_mnc r left join DM_RF.rf_country c on c.calling_code = r.country_code");
while ($res = tep_db_fetch_array($query)){
    $mcc_country[$res['mcc']]['country']= iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
    $mcc_country[$res['mcc']]['network']= iconv('UTF-8', 'UTF-8//IGNORE', $res['network']);
}
// operator
$operateur = array();
$query = tep_db_query("SELECT id, name FROM DM_RF.rf_operator;");
while($res = tep_db_fetch_array($query)) $operateur[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
// bonus_category
$bonus_cg = array();
$query = tep_db_query("SELECT id, name FROM DM_RF.rf_bonus_category;");
while($res = tep_db_fetch_array($query)) $bonus_cg[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
//recl type ref 
$rf_recl_type = array();
$query = tep_db_query("SELECT trim(type_id) id, type_libelle from DM_RF.rf_agora_type_reclam");
while ($res = tep_db_fetch_array($query)) {
    $rf_recl_type[$res['id']] = $res['type_libelle'];
}

// recl sous type ref 
$rf_recl_s_type = array();
$query = tep_db_query("SELECT trim(s_type_id) id, s_type_libelle from DM_RF.rf_agora_s_type_reclam");
while ($res = tep_db_fetch_array($query)) {
    $rf_recl_s_type[$res['id']] = $res['s_type_libelle'];
}

// Campagne ref
$rf_campagne = array();
$query = tep_db_query("SELECT trim(cmp_id) id, trim(cmp_name) name from DWH.lms_list_cmp");
while ($res = tep_db_fetch_array($query)) {
    $rf_campagne[$res['id']] = $res['name'];
}

//vas
$vas = array();
$query = tep_db_query("select code, name from DM_RF.rf_vas;");
while($res = tep_db_fetch_array($query)) $vas[$res['code']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
// tp
$offer = array();
$query = tep_db_query("SELECT description name, tmcode, b.name billing_type
FROM DM_RF.rf_tp_cbm r left join DM_RF.rf_billing_type b on b.id = r.billing_type");
while($res = tep_db_fetch_array($query)) $offer[$res['tmcode']]['name'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);	
//
$custgroup = array();
$query = tep_db_query("SELECT PRGCODE id, name, if(PRGCODE in (5,16), 'B2C','B2B') market FROM DM_RF.rf_customer_group");
while($res = tep_db_fetch_array($query)) {
    $custgroup[$res['id']]['name']  = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
    $custgroup[$res['id']]['market']  = iconv('UTF-8', 'UTF-8//IGNORE', $res['market']);
}
// recharge_type
$recharge = array();
$query = tep_db_query("SELECT code, t.name FROM DM_RF.rf_recharge_code r inner join DM_RF.rf_rec_type t on t.id = r.rec_type");
while($res = tep_db_fetch_array($query)) $recharge[$res['code']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);

// souscriptions type
$souscription = array();$souscription_id = array();
$query = tep_db_query("select r1.ocs_code code, r1.name, r1.long_name, r1.type, r1.ocs_price ocs_price, r1.payment_method, r2.ocs_code, r2.ocs_price ocs_price1
from  DM_RF.rf_subscriptions r1
left join DM_RF.rf_subscriptions r2 on r2.ocs_code = r1.code_prov and r1.code_prov <>''
where r1.ocs_code <> ''");
while($res = tep_db_fetch_array($query)) {
    $souscription[$res['code']]['name'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
    $souscription[$res['code']]['long_name'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['long_name']);
    $souscription[$res['code']]['type'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['type']);
    $souscription[$res['code']]['montant'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['ocs_price']);
	$souscription[$res['code']]['montant_ln'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['ocs_price1']);
    $souscription[$res['code']]['pm'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['payment_method']);
}
//load bundle renewal
$bundle_renew = array();
$query = tep_db_query("select ocs_code from DM_RF.rf_subscriptions where type_bundle = 'RENEWAL'");
while ($result = tep_db_fetch_array($query)) $bundle_renew[$result['ocs_code']]=1;

// event origin
$event_origin=array();
$query = tep_db_query("SELECT event_origin, description FROM DM_RF.rf_event_origin where rev_adj=1");
while($res = tep_db_fetch_array($query)) $event_origin[$res['event_origin']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['description']);

// location
$location =array();$cell=array();$cell_data=array();$cell_lac=array();$site=array();
$query = tep_db_query("SELECT sig_lac_ci, sig_zoneorange_name sig_zone, sig_nom_site, sig_id id, sig_comment, sig_cel
FROM DM_RF.rf_sig_cell_krill sig;");
while($res = tep_db_fetch_array($query)){
	 $location[$res['id']]['name'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['sig_nom_site']);
	 $location[$res['id']]['region'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['sig_zone']);
	 $location[$res['id']]['id'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['id']);
	 $cell[$res['sig_cel']]=iconv('UTF-8', 'UTF-8//IGNORE', $res['id']);
	 $site[$res['sig_cel']]=iconv('UTF-8', 'UTF-8//IGNORE', $res['sig_nom_site']);
	 $cell_data[$res['sig_cel']]=iconv('UTF-8', 'UTF-8//IGNORE', $res['sig_comment']);
}

//
$datype = array();$daunit = array();
$query = tep_db_query("SELECT da_id id, da_name, c.name cat_name ,t.name da_type, offer_id
FROM DM_RF.rf_dedicated_account d
LEFT JOIN DM_RF.rf_da_type t ON t.id = d.da_type
LEFT JOIN DM_RF.rf_da_category c ON c.id = d.da_category ;");
while($res = tep_db_fetch_array($query)) {
$datype[$res['id']][$res['offer_id']]['name'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['da_name']); 
   $datype[$res['id']][$res['offer_id']]['type'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['da_type']); 
   $datype[$res['id']][$res['offer_id']]['category'] = iconv('UTF-8', 'UTF-8//IGNORE', $res['cat_name']);
}
$query = tep_db_query("SELECT da_unit id, da_name name  FROM DM_RF.rf_da_unite");
while($res = tep_db_fetch_array($query)) {
    $daunit[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);    
}
//
$bundle_group = array();
$query = tep_db_query("select id, name from DM_RF.rf_type_subscriptions;");
while($res = tep_db_fetch_array($query)) $bundle_group[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
//
$pt = array();
$query = tep_db_query("SELECT id, name FROM DM_RF.rf_type_payment;");
while($res = tep_db_fetch_array($query)) $pt[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
echo "fin du traitement ref:" . (time() - $d0) ."\n";
echo "load caller " . (time() - $d0) ."\n";
//initialisation customer
$customer = array();
//localisation postpaid

$query = tep_db_query("select concat('261',msisdn) msisdn, site_id  FROM DM_OD.caller_postpaid_evt where site_id > 0");
while($res = tep_db_fetch_array($query))   $customer[$res['msisdn']]['loc_code'] = $res['site_id'];

//localisation prepaid
$query = tep_db_query("select concat('261',msisdn) msisdn, site_id  FROM DM_OD.caller_orange");
while($res = tep_db_fetch_array($query))   $customer[$res['msisdn']]['loc_code'] = $res['site_id'];

$query = tep_db_query("select id, name  FROM DM_RF.rf_billing_type");
while($res = tep_db_fetch_array($query))   $btype[$res['id']] = iconv('UTF-8', 'UTF-8//IGNORE', $res['name']);
echo "load bscs " . (time() - $d0) ."\n";

$query = tep_db_query("SELECT t1.rsc_ressource msisdn, t3.cust_group prg_code, ctr_pta tmcode, cust_code, ctr_type billing_type, t2.ctr_id
FROM DWH.bscs_contract_ressource_rsc t1
INNER JOIN DWH.bscs_contract_ctr t2 ON t1.rsc_contract_id = t2.ctr_id
INNER JOIN DWH.bscs_customer_cust t3 ON t3.cust_id = t2.ctr_cust_id
WHERE t1.rsc_ressource <> ''
ORDER BY t1.rsc_activ_date");
while($res= tep_db_fetch_array($query)){
    $customer[$res['msisdn']]['prg_code'] = $res['prg_code'];
    $customer[$res['msisdn']]['tmcode'] = $res['tmcode'];
    $customer[$res['msisdn']]['billing_type'] = $res['billing_type'];
    $customer[$res['msisdn']]['subs_id'] = $res['ctr_id'];
	$customer[$res['msisdn']]['cust_code'] = $res['cust_code'];
}

//main
echo "Database cbm selected\n";

echo "Collection created succsessfully\n";
$tv=0;
for ($year=$yd; $year<=$yf; $year++) {
    for ($mois =intval($md); $mois<=intval($mf); $mois++) {
        $mo=$mois ;
        if ($mois<10) $mo='0'.$mois ;
        $themois =$year.$mo;
        for ($j=intval($jd);$j<=intval($jf);$j++) {
            tep_db_connect();
            $cbm =array();$cbm1 =array();$region_list=array();$cbm_roaming=array();$cbm_roaming1=array();$imei_list=array();$last_imei=array();$cbm_da = array();$cbm1_da = array();$cbmr_da=array();$cbmr_da1=array();
            
            $d = $year.'-' . $mo . '-'  . $j;
            if ($j<10) 	$d = $year.'-' . $mo . '-0' . $j;
            $db = $d . ' 00:00:00';
            $df = $d . ' 23:59:59';
            echo "Debut " . $db . ' - ' .  date("D, H:i:s") . "\t";
            $dj = $year . $mo .$j;
            if ($j<10) 	$dj = $year.$mo . '0' . $j;
            $time_jour = strtotime($db);
            $time_finjour = strtotime($df);
            $mgdate = strtotime($db); //+ 3600;
            $t = time();  
			//tep_db_connect1();
            $query = tep_db_query1("SELECT  OCS_EVENT_DATE, OCS_NODE_TYPE, OCS_RECORD_TYPE,  OCS_OFFER_ID, OCS_CALLING_MSISDN , OCS_CALLED_MSISDN , OCS_ROAMING_FLAG ,OCS_AMOUNT , OCS_DURATION , OCS_RATED_FREE ,concat(OCS_MCC,OCS_MNC) mcc , 
                abs(OCS_VALUE_BEFORE-OCS_VALUE_AFTER)  as amount, OCS_CELL_ID cell, OCS_SERVICE_CLASS , OCS_DA_ID , OCS_DA_QTY , OCS_DA_UNIT,OCS_DA_DURATION,OCS_TRANSACTION_CODE, OCS_EVENT_ORIGIN
                FROM DWH.cdr_rated WHERE OCS_EVENT_DATE BETWEEN '" . $db ."' AND '" . $df ."' 
				-- and OCS_NODE_TYPE ='AIR_Refill' AND ocs_transaction_code ='FE30'
				-- and OCS_CALLING_MSISDN='261324723941'");            
            
			/*$query = tep_db_query1("SELECT * FROM DWH2019.cdr_rated2 PARTITION (P20191101)
			WHERE ocs_event_date = '2019-10-31 02:31:05' AND ocs_calling_msisdn = '261320201383' AND ocs_node_type ='AIR_Refill' AND ocs_transaction_code IN ('YT12','B050','FE30')
			;"); */
			$record=0;
           
            $bulk = new MongoDB\Driver\BulkWrite();
            $fact = 1;
            $purchase_iter = 0;
            while ($res=tep_db_fetch_array($query)) {
                    $record++;
                    $cdr_type = $res['OCS_NODE_TYPE'];
                    $ocs_record_type = $res['OCS_RECORD_TYPE'];
                    $ocs_event_date = $res['OCS_EVENT_DATE'];
                    $ocs_calling_msisdn = $res['OCS_CALLING_MSISDN'];
                    $ocs_called_msisdn = $res['OCS_CALLED_MSISDN'];
                    $ocs_roaming_flag = $res['OCS_ROAMING_FLAG'];
                    $ocs_amount =$res['OCS_AMOUNT'];
                    $ocs_duration = $res['OCS_DURATION'];
                    $ocs_rated_free = $res['OCS_RATED_FREE'];
                    $ocs_mcc = $res['mcc'];
                    $amount = $res['amount'];
                    $ocs_cell = $res['cell'];
                    $ocs_service_class = $res['OCS_SERVICE_CLASS'];
                    $ocs_offer_id = $res['OCS_OFFER_ID'];
                    $ocs_da_id = $res['OCS_DA_ID'];
					$ocs_da_qty = $res['OCS_DA_QTY'];
                    $ocs_da_unit = $res['OCS_DA_UNIT'];
                    $ocs_da_duration = $res['OCS_DA_DURATION'];
                    $ocs_transaction_code = $res['OCS_TRANSACTION_CODE'];
                    $ocs_event_origin = $res['OCS_EVENT_ORIGIN']; 
                						
                if (($cdr_type=='CCN_VOICE') || ($cdr_type=='CCN_SMS') ||  ($cdr_type=='OCC_DATA') || ($cdr_type=='AIR_Refill')) {
                    if (($ocs_amount<9999999) && ($ocs_amount<9999999) && ($ocs_amount>=0)){
                        if ( (($cdr_type =='CCN_VOICE') || ($cdr_type =='CCN_SMS')) && ((strlen($ocs_called_msisdn)>13) && (substr($ocs_called_msisdn,0,3)==202))) {
                        } else {
                            $subclass = '';
                            if (substr($ocs_called_msisdn,0,3) == 261) {
                                //Local
                                $called = substr($ocs_called_msisdn,3);
                                $op_id = operator($called);
                                $subclass = 261;
                                if (isset($vas[$called])) {
                                    $subclass = $called;
                                    $op_id = 12;
                                } 
                            } else {
                                //International
                                $op_id = 99;
                                $subclass = country($ocs_called_msisdn);
                            }
                            $msisdn = $ocs_calling_msisdn;
                            $num2 =  $ocs_called_msisdn;
                            $roaming_flag = $ocs_roaming_flag;
                            $way = 1;
                            if ($ocs_record_type == 'MTCROM') $way = 2;
                            if($ocs_cell !='-') $typedata =$cell_data[$ocs_cell];
                            $site_id = intval($cell[$ocs_cell]) ;
                            $profile_id = $ocs_transaction_code;
                            $duration = intval($ocs_duration);
                            $duration_free = 0;
                            $mcc = $ocs_mcc;
                            if ($site_id >0) {
                                $customer[$msisdn]['site_id'] = $site_id;
                                $region_id = $location[$site_id]['region'];
                                if($region_id != '') $region_list[$msisdn][$region_id] =1;
                            } else {
                                if (isset( $customer[$msisdn]['site_id'])) $site_id =  $customer[$msisdn]['site_id'];
                            }
                            $tva = 1;
                            if($ocs_service_class <5000) $tva=1.2;
                            $charge = $tva*$ocs_amount;
                            $pyg_amount = $tva*$amount;
                            $tp_id = $ocs_service_class;$fees=0;$loan=0;
                            //DA calculation
                            $da = array();$da1 =array();$da2 =array();
                            $da1 = explode('+',$ocs_da_id);
                            for ($i = 0; $i< sizeof($da1); $i++) $da[$i]['da_id'] = $da1[$i];
                            if($cdr_type =='CCN_VOICE') {
                                $da2 = explode('+',$ocs_da_duration);
                                for ($i = 0; $i< sizeof($da1); $i++) {
                                    $du = explode('|',$da2[$i]);
                                    $duration -= $du[1];
                                    $da[$i]['da_id'] = $du[0];
                                    $da[$i]['da_duration'] += $du[1];
                                    $duration_free += $du[1];
                                }
                                $duration = max(0,$duration);
                                $totaldurationpyg += $duration;
                                $totaldurationfree += $duration_free;
                            }
                            $da2 =array();
                            $da2 = explode('+',$ocs_da_unit);
                            for ($i = 0; $i< sizeof($da1); $i++) $da[$i]['da_unit'] = $da2[$i];
                            $da2 =array();
                            $da2 = explode('+',$ocs_da_qty);
                            for ($i = 0; $i< sizeof($da1); $i++) $da[$i]['da_qty'] = $da2[$i];
							$da2 =array();
                            $da2 = explode('+',$ocs_offer_id);
                            for ($i = 0; $i< sizeof($da1); $i++) $da[$i]['offer_id'] = $da2[$i];
                            $cbm[$msisdn][99]=0;$cbm1[$msisdn][99]=0;
                            switch ($cdr_type) {
                                case 'CCN_VOICE': // voice
                                    if($roaming_flag !=2)  {
                                        if($op_id!=12) { 
                                            $cbm[$msisdn][1][$op_id][$site_id]['voice_o_cnt'] ++;
                                            if($op_id=='99') $cbm1[$msisdn][$subclass][$site_id]['voice_o_cnt'] ++;
                                        }else {
                                            $cbm[$msisdn][1][1][$site_id]['voice_vas_cnt'] ++;
                                        }
                                        if($op_id!=12) {	
                                            if($duration > 0) $cbm[$msisdn][1][$op_id][$site_id]['voice_o_main_vol'] += $duration;
                                            if($pyg_amount <> 0) $cbm[$msisdn][1][$op_id][$site_id]['voice_o_amnt'] += $pyg_amount;
                                            if($charge <> 0) $cbm[$msisdn][1][$op_id][$site_id]['voice_o_amnt1'] += $charge;
                                            if($op_id=='99') {
                                                if($duration > 0) $cbm1[$msisdn][$subclass][$site_id]['voice_o_main_vol'] += $duration;
                                                if($pyg_amount <> 0) $cbm1[$msisdn][$subclass][$site_id]['voice_o_amnt'] += $pyg_amount;
                                                if($charge <> 0) $cbm1[$msisdn][$subclass][$site_id]['voice_o_amnt1'] += $charge;
                                            }
                                        }else {
                                            if($pyg_amount <> 0) $cbm[$msisdn][1][1][$site_id]['voice_vas_amnt'] += $pyg_amount;// count non disponible a ajouter
                                            if($charge <> 0) $cbm[$msisdn][1][1][$site_id]['voice_vas_amnt1'] += $charge;// count non disponible a ajouter
                                            if($duration >0) $cbm[$msisdn][1][1][$site_id]['voice_vas_main_vol'] += $duration;
                                        }
                                        if(!empty($da)) {
                                            if($op_id!=12) {	
                                                $cbm[$msisdn][1][$op_id][$site_id]['voice_o_bndl_vol'] += $duration_free;
                                                if($op_id=='99') $cbm1[$msisdn][$subclass][$site_id]['voice_o_bndl_vol'] += $duration_free;
                                            }else {
                                                $cbm[$msisdn][1][1][$site_id]['voice_vas_bndl_vol'] += $duration_free;
                                            }
                                            foreach ($da as $i => &$value){
                                                if($op_id!=12) {
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm[$msisdn][1][$op_id][$site_id]['voice_o_ln'] += $value['da_qty'];
                                                    }
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data                                                                                
                                                        $cbm[$msisdn][1][$op_id][$site_id]['voice_o_ld'] += $value['da_qty'];
                                                    }*/  										
													//da_offer
                                                    if($value['da_duration'] >0)  $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['vol'] += $value['da_duration'];
                                                    if($value['da_qty'] >0)  $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                    $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                }else{
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm[$msisdn][1][1][$site_id]['voice_vas_ln'] += $value['da_qty'];
                                                    }
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data                                                                                                                                                          
                                                        $cbm[$msisdn][1][1][$site_id]['voice_vas_ld'] += $value['da_qty'];
                                                    } */
                                                    if($value['da_duration'] >0)  $cbm_da[$msisdn][1][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['vol'] += $value['da_duration']; 
                                                    if($value['da_qty'] >0)  $cbm_da[$msisdn][1][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                    $cbm_da[$msisdn][1][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                }
                                                if($op_id =='99') {
                                                   if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm1[$msisdn][$subclass][$site_id]['voice_o_ln'] += $value['da_qty'];
                                                    }
													//pas de revenue PYG lany data
													/*if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data                                                                                                                                                                                                                                              
                                                        $cbm1[$msisdn][$subclass][$site_id]['voice_o_ld'] += $value['da_qty'];
                                                    }*/													
                                                    if($value['da_duration'] >0) $cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['vol'] += $value['da_duration'];  
                                                    if($value['da_qty'] >0) $cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                    $cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                }
                                            }
                                        }
                                    } else {
                                        $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_cnt'] ++;
                                        if($op_id=='99') $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_cnt'] ++;
                                        if($duration >0) $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_main_vol'] += $duration;
                                        if($pyg_amount <>0) $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_amnt'] += $pyg_amount;
                                        if($charge <>0) $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_amnt1'] += $charge;
                                        if($op_id =='99') {
                                            if($duration >0) $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_main_vol'] += $duration;
                                            if($pyg_amount <>0) $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_amnt'] += $pyg_amount;
                                            if($charge <>0) $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_amnt1'] += $charge;
                                        }
                                        if(!empty($da)) {
                                            if($duration_free >0) {
                                                $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_bndl_vol'] += $duration_free; 
                                                if($op_id=='99') $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_bndl_vol'] += $duration_free;
                                            }
                                            foreach ($da as $i => &$value){
                                                if($value['da_id']==31) { // event sur lany credit                                                                                
                                                    $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_ln'] += $value['da_qty'];                                                    
                                                }
												//pas de revenue PYG lany data
												/*if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data                                                                                                                                                                                                                                                                                                                                
                                                    $cbm_roaming[$msisdn][$mcc][$op_id]['voice_o_ld'] += $value['da_qty'];                                                    
                                                }*/
                                                if($value['da_duration'] >0) $cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['vol'] += $value['da_duration']; 
                                                if($value['da_qty'] >0) $cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty']; 
                                                $cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                if($op_id =='99') {
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_ln'] += $value['da_qty'];
                                                    }
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data                                                                                                                                                                                                                                                
                                                        $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_ld'] += $value['da_qty'];
                                                    }*/
													if($value['da_duration'] >0)$cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['vol'] += $value['da_duration'];
                                                    if($value['da_qty'] >0)$cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                    $cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][1][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                }
                                            }
                                        }
                                    }
                                break;

                                case 'CCN_SMS' : // sms
                                    if($roaming_flag !=2)  {
                                        if($op_id!=12) { 
                                            if($pyg_amount <>0) $cbm[$msisdn][1][$op_id][$site_id]['sms_o_main_cnt'] ++;
                                            if($pyg_amount <>0) $cbm[$msisdn][1][$op_id][$site_id]['sms_o_amnt'] += $pyg_amount;
                                            if($charge <>0) $cbm[$msisdn][1][$op_id][$site_id]['sms_o_amnt1'] += $charge;
                                            if($op_id=='99') {
                                                if($pyg_amount <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_main_cnt'] ++;
                                                if($pyg_amount <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_amnt'] += $pyg_amount;
                                                if($charge <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_amnt1'] += $charge;
                                            }
                                        }else {
                                            if($pyg_amount <>0) $cbm[$msisdn][1][1][$site_id]['sms_vas_cnt'] ++;
                                            if($pyg_amount <>0) $cbm[$msisdn][1][1][$site_id]['sms_vas_amnt'] += $pyg_amount;
                                            if($charge <>0) $cbm[$msisdn][1][1][$site_id]['sms_vas_amnt1'] += $charge;
                                            if($op_id=='99') {
                                                if($pyg_amount <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_main_cnt'] ++;
                                                if($pyg_amount <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_amnt'] += $pyg_amount;
                                                if($charge <>0) $cbm1[$msisdn][$subclass][$site_id]['sms_o_amnt1'] += $charge;
                                            }
                                        }
                                        if(!empty($da)) {
                                            $qty=0;
                                            foreach ($da as $i => &$value){
                                                $qty ++;
                                                if($op_id!=12) {
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm[$msisdn][1][$op_id][$site_id]['sms_o_ln'] += $value['da_qty'];
                                                    } 
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
													    $cbm[$msisdn][1][$op_id][$site_id]['sms_o_ld'] += $value['da_qty'];
                                                    }*/
                                                    $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                                    $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['vol'] ++;
                                                    if($value['da_qty'] >0) $cbm_da[$msisdn][$op_id][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                }else{
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm[$msisdn][1][1][$site_id]['sms_vas_ln'] += $value['da_qty'];
                                                    }     
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
													    $cbm[$msisdn][1][1][$site_id]['sms_vas_ld'] += $value['da_qty'];
                                                    } */
                                                    $cbm_da[$msisdn][1][$site_id][$value['da_id']][2][$value['da_unit']]['cnt'] ++;  
                                                    $cbm_da[$msisdn][1][$site_id][$value['da_id']][2][$value['da_unit']]['vol'] ++;  
                                                    if($value['da_qty'] >0) $cbm_da[$msisdn][1][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                }
                                                if($op_id =='99') {
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                        $cbm1[$msisdn][$subclass][$site_id]['sms_o_ln'] += $value['da_qty'];
                                                    }   
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
													    $cbm1[$msisdn][$subclass][$site_id]['sms_o_ld'] += $value['da_qty'];
                                                    }*/
                                                    $cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['cnt'] ++;
													$cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['vol'] ++;
                                                    if($value['da_qty'] >0) $cbm1_da[$msisdn][$subclass][$site_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                }
                                            }
                                            if($op_id!=12) {	
                                                if($pyg_amount == 0) $cbm[$msisdn][1][$op_id][$site_id]['sms_o_bndl_cnt'] ++;
                                                if(($op_id=='99')&&($pyg_amount == 0)) $cbm1[$msisdn][$subclass][$site_id]['sms_o_bndl_cnt'] ++;
                                            }else {
                                                if($pyg_amount == 0) $cbm[$msisdn][1][1][$site_id]['sms_vas_bndl_cnt'] ++;
                                            }
                                        }
                                    } else {
                                        if($pyg_amount <>0) $cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_main_cnt'] ++;
                                        if(($op_id=='99') && ($pyg_amount <>0))$cbm_roaming1[$msisdn][$mcc][$subclass]['sms_o_main_cnt'] ++;
                                        if($pyg_amount <>0) $cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_amnt'] += $pyg_amount;
                                        if($charge <>0) $cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_amnt1'] += $charge;
                                        if($op_id =='99') {
                                            if($pyg_amount <>0) $cbm_roaming1[$msisdn][$mcc][$subclass]['sms_o_amnt'] += $pyg_amount;
                                            if($charge <>0) $cbm_roaming1[$msisdn][$mcc][$subclass]['sms_o_amnt1'] += $charge;
                                        }
                                        if(!empty($da)) {
                                            $qty=0;
                                            foreach ($da as $i => &$value){	
                                                $qty++;
                                                $cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['cnt'] ++;
												$cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['vol'] ++;
                                                if($value['da_id']==31) { // event sur lany credit                                                                                
                                                    $cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_ln'] += $value['da_qty'];
                                                }
												//pas de revenue PYG lany data
												/*
												if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
												    $cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_ld'] += $value['da_qty'];
                                                }*/
                                                if($value['da_qty'] >0) $cbmr_da[$msisdn][$mcc][$op_id][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                if($op_id =='99') {
                                                    if($value['da_id']==31) { // event sur lany credit                                                                                
                                                       $cbm_roaming1[$msisdn][$mcc][$subclass]['sms_o_ln'] += $value['da_qty'];                                                 
                                                    }
													//pas de revenue PYG lany data
													/*
													if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
														$cbm_roaming1[$msisdn][$mcc][$subclass]['sms_o_ld'] += $value['da_qty'];
													}*/
                                                    $cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['cnt'] ++; 
                                                    $cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['vol'] ++; 
                                                    if($value['da_qty'] >0) $cbmr_da1[$msisdn][$mcc][$subclass][$value['da_id']][2][$value['offer_id']][$value['da_unit']]['qty'] += $value['da_qty'];
                                                }
                                            }
                                            if($pyg_amount == 0)$cbm_roaming[$msisdn][$mcc][$op_id]['sms_o_bndl_cnt'] ++; 
                                            if (($op_id=='99')&&($pyg_amount == 0)) $cbm1_roaming[$msisdn][$mcc][$subclass]['sms_o_bndl_cnt'] ++;
                                        }
                                    }
                                break;

                                case 'OCC_DATA' : // data
                                    $ttv=0;
                                    $volume = $duration;
                                    if($roaming_flag !=2)  {
                                        if($pyg_amount <>0) {  
                                            if($duration >0)  {
                                                $cbm[$msisdn][1][1][$site_id]['data_main_vol'] += $duration;
                                                $cbm[$msisdn][1][1][$site_id][$typedata] += $duration;
                                            }
                                            $cbm[$msisdn][1][1][$site_id]['data_amnt'] += $pyg_amount;
                                        }else{
                                            if($duration >0) {
                                                $cbm[$msisdn][1][1][$site_id]['data_bndl_vol'] += $duration;
                                                $cbm[$msisdn][1][1][$site_id][$typedata] += $duration;
                                            }
                                        }              
                                        if($charge <>0)   $cbm[$msisdn][1][1][$site_id]['data_amnt1'] += $charge;
                                        if(!empty($da)) {
                                            foreach ($da as $i => &$value){
                                                if($value['da_id']==31) { // event sur lany credit                                                                                
                                                    $cbm[$msisdn][1][1][$site_id]['data_ln'] += $value['da_qty'];
                                                }  
												//pas de revenue PYG lany data
												/*if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
												    $cbm[$msisdn][1][1][$site_id]['data_ld'] += $value['da_qty'];
                                                } */
                                                if ($value['da_unit'] == 1) {
                                                    $cbm_da[$msisdn][1][$site_id][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['vol'] +=$volume;
                                                    $volume=0;                                                    
                                                }
                                                if ($value['da_unit'] == 6) $cbm_da[$msisdn][1][$site_id][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['vol'] +=$value['da_qty'];
                                                if($value['da_qty'] >0)     $cbm_da[$msisdn][1][$site_id][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['qty'] +=$value['da_qty'];
                                                $cbm_da[$msisdn][1][$site_id][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                            }
                                        }
                                    } else {
                                        if($pyg_amount <>0) {  
                                            if($duration >0)$cbm_roaming[$msisdn][$mcc][1]['data_main_vol']  += $duration;
                                            $cbm_roaming[$msisdn][$mcc][1]['data_amnt'] += $pyg_amount;
                                        }else{
                                            if($duration >0) $cbm_roaming[$msisdn][$mcc][1]['data_bndl_vol'] += $duration;
                                        }
                                        if($charge <>0) $cbm_roaming[$msisdn][$mcc][1]['data_amnt1'] += $charge;
                                        if(!empty($da)) {
                                            $qty=0;
                                            foreach ($da as $i => &$value){
                                                if($value['da_id']==31) { // event sur lany credit                                                                                
                                                    $cbm_roaming[$msisdn][$mcc][1]['data_ln'] += $value['da_qty'];
                                                }
												//pas de revenue PYG lany data
												/*
												if(($value['da_id']==202 && $value['offer_id']==8104) || ($value['da_id']==282 && $value['offer_id']==8138) || ($value['da_id']==427 && $value['offer_id']==8190)) { // event sur lany data
												    $cbm_roaming[$msisdn][$mcc][1]['data_ld'] += $value['da_qty'];
                                                }*/												
                                                if ($value['da_unit'] == 1) {
                                                    $cbmr_da[$msisdn][1][$site_id][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['vol'] +=$volume;                                                   
                                                    $volume=0;                                                    
                                                }                                                
                                                if ($value['da_unit'] == 6) $cbmr_da[$msisdn][$mcc][1][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['vol'] +=$value['da_qty'];
                                                if($value['da_qty'] >0)     $cbmr_da[$msisdn][$mcc][1][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['qty'] +=$value['da_qty'];
                                                $cbmr_da[$msisdn][$mcc][1][$value['da_id']][3][$value['offer_id']][$value['da_unit']]['cnt'] ++;
                                            }
                                        }
                                    }
                                break;

                                case 'AIR_Refill': // refill + subs +EC
                                    if (($profile_id == 'BJ') || ($profile_id == 'DB19')|| ($profile_id == 'BT'))  $charge =0; //
                                    switch($profile_id) {
                                        case 'OH': //lany credit
                                        case 'OI':
                                        case 'OJ':
                                        case 'OQ':
                                        case 'IJ':   
                                        case 'IH':
                                        case 'RE':							
                                      
                                            $fees=0;$loan=0;
                                            if(!empty($da)) {
                                                foreach ($da as $i => &$value){
                                                    if($value['da_id'] == 32) {
                                                        $fees = abs($value['da_qty']);
                                                    } elseif($value['da_id'] == 33){
                                                        $loan = abs($value['da_qty']);
                                                    } elseif($value['da_id'] ==998){	
                                                        $loan = abs($value['da_qty']);
                                                    }
                                                }
                                                if($loan >0) {
                                                    $cbm[$msisdn][9][1][$site_id]['ec_loan'] += $loan;
                                                    $cbm[$msisdn][9][1][$site_id]['ec_qty'] ++;
                                                    $document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
                                                                      "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
                                                                      "party_id" => strval($msisdn),
                                                                      "subs_id" => $customer[$msisdn]['subs_id'],
																	  "cust_code" => $customer[$msisdn]['cust_code'],
                                                                      "billing_type" => $btype[$customer[$msisdn]['billing_type']],
                                                                      "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
                                                                      "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
                                                                      "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
                                                                      "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
                                                                      "pur_name" => 'recharge EC',
                                                                      "pur_code" => $profile_id,
                                                                      "pur_type" => $profile_id == 'RE' ? 'reativation': $recharge[$profile_id],
                                                                      "pur_amnt" => floatval($loan),
                                                                      "pur_fees" => floatval($fees));
                                                    
                                                    if($purchase_iter < 10000)  {
                                                        $bulk->insert($document);
                                                        $purchase_iter++;
                                                    }
                                                    else {
                                                        $bulk->insert($document);
                                                        $mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
                                                        if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
                                                        $bulk = new MongoDB\Driver\BulkWrite();
                                                        $purchase_iter = 0;
                                                        $fact++;
                                                    }
                                                    //$collection_purchase->insert($document);
                                                }
                                            }
                                        break; 
											//Lany Data
										  case 'YT12':
										  case 'B050':
										  case 'FE30': 
											if($charge >0) { // refill
												$fees=0;$loan=0;
												if(!empty($da)) {
													foreach ($da as $i => &$value){
														if($value['da_id'] == 1032) {
															$fees = abs($value['da_qty']);
														} elseif($value['da_id'] == 1033){
															$loan = abs($value['da_qty']);
														} 
													}
													if($loan >0) {
														$cbm[$msisdn][9][1][$site_id]['ed_loan'] += $loan;
														$cbm[$msisdn][9][1][$site_id]['ed_qty'] ++;
														$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																		  "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																		  "party_id" => strval($msisdn),
																		  "subs_id" => $customer[$msisdn]['subs_id'],
																		  "cust_code" => $customer[$msisdn]['cust_code'],
																		  "billing_type" => $btype[$customer[$msisdn]['billing_type']],
																		  "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																		  "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																		  "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
																		  "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																		  "pur_name" => 'recharge ED',
																		  "pur_code" => $profile_id,
																		  "pur_type" => $profile_id == 'RE' ? 'reativation': $recharge[$profile_id],
																		  "pur_amnt" => floatval($loan),
																		  "pur_fees" => floatval($fees));
														
														if($purchase_iter < 10000)  {
															$bulk->insert($document);
															$purchase_iter++;
														}
														else {
															$bulk->insert($document);
															$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
															if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
															$bulk = new MongoDB\Driver\BulkWrite();
															$purchase_iter = 0;
															$fact++;
														}
														//$collection_purchase->insert($document);
													}
												}
											}
											else{ // bundle
												if(!empty($da) && $da[0]['da_id'] !='') {
													$pur_amnt = 0;
													$pur_amnt = $souscription[$profile_id]['montant'];
													$cbm[$msisdn][3][$profile_id][$site_id]['revenue'] += $pur_amnt;
													$cbm[$msisdn][3][$profile_id][$site_id]['qty'] ++;
													//echo "insert bundle LD ". $profile_id . " " . $msisdn . " da: " . $da[0]['da_id'] . "\n";
													//if($pt[$souscription[$profile_id]['pm']] == 'Lany Credit' && $ocs_record_type == 'IVR') $pur_amnt_ln =floatval($souscription[$profile_id]['montant_ln']); 
													$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																	   "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																	   "party_id" => strval($msisdn),
																	   "subs_id" => $customer[$msisdn]['subs_id'],
																	   "cust_code" => $customer[$msisdn]['cust_code'],
																	   "billing_type" => $btype[$customer[$msisdn]['billing_type']],
																	   "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																	   "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																	   "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
																	   "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																	   "pur_name" => 'bundle',
																	   "pur_code" => $profile_id,
																	   "pur_bndle" => $souscription[$profile_id]['name'],
																		"pur_bndle_longname" => $souscription[$profile_id]['long_name'],
																	   "pur_group" =>$bundle_group[$souscription[$profile_id]['type']],
																	   "pur_payment_type" =>$pt[$souscription[$profile_id]['pm']],
																	   "pur_amnt" => floatval($pur_amnt),
																	   "pur_amnt_ln" => $pur_amnt_ln);
													
														if($purchase_iter < 10000)  {
															$bulk->insert($document);
															$purchase_iter++;
														}
														else {
															$bulk->insert($document);
															$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
															if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
															$bulk = new MongoDB\Driver\BulkWrite();
															$purchase_iter = 0;
															$fact++;
														}
													
												}
											}
                                        break; 
                                        default: //refill + subs
                                            if($charge >0) { // refill
                                                $cbm[$msisdn][2][$profile_id][$site_id]['qty'] ++;
                                                $cbm[$msisdn][2][$profile_id][$site_id]['revenue'] += abs($charge/$tva);
                                                $fees=0;$payback=0;$ca_reactivation=0;$fees_ed=0;$payback_ed=0;
                                                if(!empty($da)) {
                                                    foreach ($da as $i => &$value){
                                                        if($value['da_id'] == 32) {
                                                            $fees = abs($value['da_qty']);
                                                        } elseif($value['da_id'] == 33){
                                                            $payback = abs($value['da_qty']);	
                                                        }elseif($value['da_id'] == 998){
                                                            if($profile_id !='RE') {  // CA renouvellement
                                                                $ca_reactivation += abs($value['da_qty']);
                                                            }															
                                                        }
														//lany data info stockée dans da_unit
														if($value['da_id'] == 1033){
                                                            $payback_ed = abs($value['da_qty']);	
                                                        }elseif($value['da_id'] == 1032){
                                                            $fees_ed = abs($value['da_qty']);	
                                                        }
														
                                                    }
                                                    if($fees+$payback >0) {
                                                        $cbm[$msisdn][9][1][$site_id]['ec_payback'] += $payback;
                                                        $cbm[$msisdn][9][1][$site_id]['ec_fees'] += $fees;                                                                          
                                                    }
													if($fees_ed+$payback_ed >0) {
                                                        $cbm[$msisdn][9][1][$site_id]['ed_payback'] += $payback_ed;
                                                        $cbm[$msisdn][9][1][$site_id]['ed_fees'] += $fees_ed;                                                                          
                                                    }
                                                    if($ca_reactivation >0) {                                                    
                                                        $cbm[$msisdn][9][1][$site_id]['ca_reactivation'] += $ca_reactivation; 
                                                    }
                                                }
												$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																"purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																"party_id" => strval($msisdn),
																"subs_id" => $customer[$msisdn]['subs_id'],
																"cust_code" => $customer[$msisdn]['cust_code'],
																"billing_type" => $btype[$customer[$msisdn]['billing_type']],
																"market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																"loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																"loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
																"pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																"pur_name" => $profile_id == 'RE' ? 'reactivation': 'recharge',
																"pur_code" => $profile_id,
																"pur_type" => $profile_id == 'RE' ? 'reactivation': $recharge[$profile_id],
																"pur_amnt" => floatval($charge/$tva),
																"pur_fees" => floatval($fees + $fees_ed));
												if($purchase_iter < 10000)  {
													$bulk->insert($document);
													$purchase_iter++;
												}
												else {
													$bulk->insert($document);
													$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
													if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
													$bulk = new MongoDB\Driver\BulkWrite();
													$purchase_iter = 0;
													$fact++;
												}
                                                //$collection_purchase->insert($document);
                                            }else{ // bundle
												//echo "profile_id:" . $profile_id . " pm: " . $souscription[$profile_id]['pm'] . " da: " . $da[0]['da_id'] . "\n";
												//if (($souscription[$profile_id]['pm'] == 5 && $da[0]['da_id'] !='') || $souscription[$profile_id]['pm'] != 5) {
													$pur_amnt_ln =0;
													$pur_amnt = 0;
													$cbm[$msisdn][3][$profile_id][$site_id]['qty'] ++;
													//recharge MyPOS & ZEBRA
													if ($ocs_event_origin =='MYPOS') {
														//echo "IN\n";
														$profile1_id = 'MYP';
														$pur_amnt = $souscription[$profile_id]['montant']; // montant de la recharge virtuelle = montant du bundle
														$cbm[$msisdn][2][$profile1_id][$site_id]['qty'] ++;
														$cbm[$msisdn][2][$profile1_id][$site_id]['revenue'] += $pur_amnt;
														$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																	"purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																	"party_id" => strval($msisdn),
																	"subs_id" => $customer[$msisdn]['subs_id'],
																	"cust_code" => $customer[$msisdn]['cust_code'],
																	"billing_type" => $btype[$customer[$msisdn]['billing_type']],
																	"market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																	"loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																	"loc_purchase" => isset($customer[$msisdn]['site_id']) ? $customer[$msisdn]['site_id']:$location[$customer[$msisdn]['loc_code']]['name'],
																	"pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																	"pur_name" => 'recharge',
																	"pur_code" => $profile1_id,
																	"pur_type" => $recharge[$profile1_id],
																	"pur_amnt" => floatval($pur_amnt));
														 if($purchase_iter < 10000)  {
															$bulk->insert($document);
															$purchase_iter++;
														}
														else {
															$bulk->insert($document);
															$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
															if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
															$bulk = new MongoDB\Driver\BulkWrite();
															$purchase_iter = 0;
															$fact++;
														} 
													}
													if ($ocs_event_origin == 'ZEBRASERVICE') {
														$profile1_id = 'ZEB';
														$pur_amnt = $souscription[$profile_id]['montant']; // montant de la recharge virtuelle = montant du bundle
														$cbm[$msisdn][2][$profile1_id][$site_id]['qty'] ++;
														$cbm[$msisdn][2][$profile1_id][$site_id]['revenue'] += $pur_amnt;
														$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																	"purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																	"party_id" => strval($msisdn),
																	"subs_id" => $customer[$msisdn]['subs_id'],
																	"cust_code" => $customer[$msisdn]['cust_code'],
																	"billing_type" => $btype[$customer[$msisdn]['billing_type']],
																	"market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																	"loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																	"loc_purchase" => isset($customer[$msisdn]['site_id']) ? $customer[$msisdn]['site_id']:$location[$customer[$msisdn]['loc_code']]['name'],
																	"pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																	"pur_name" => 'recharge',
																	"pur_code" => $profile1_id,
																	"pur_type" => $recharge[$profile1_id],
																	"pur_amnt" => floatval($pur_amnt));
														 if($purchase_iter < 10000)  {
															$bulk->insert($document);
															$purchase_iter++;
														}
														else {
															$bulk->insert($document);
															$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
															if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
															$bulk = new MongoDB\Driver\BulkWrite();
															$purchase_iter = 0;
															$fact++;
														} 
													}
													//modification pour la partie lany credit
													if ($ocs_record_type != 'IVR' || $pt[$souscription[$profile_id]['pm']] != 'Lany Credit') $pur_amnt = $souscription[$profile_id]['montant'];
													$cbm[$msisdn][3][$profile_id][$site_id]['revenue'] += $pur_amnt;
													if($pt[$souscription[$profile_id]['pm']] == 'Lany Credit' && $ocs_record_type == 'IVR') $pur_amnt_ln =floatval($souscription[$profile_id]['montant_ln']); 
													$document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
																	   "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($ocs_event_date)*1000 +$offset),
																	   "party_id" => strval($msisdn),
																	   "subs_id" => $customer[$msisdn]['subs_id'],
																	   "cust_code" => $customer[$msisdn]['cust_code'],
																	   "billing_type" => $btype[$customer[$msisdn]['billing_type']],
																	   "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
																	   "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
																	   "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
																	   "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
																	   "pur_name" => 'bundle',
																	   "pur_code" => $profile_id,
																	   "pur_bndle" => $souscription[$profile_id]['name'],
																		"pur_bndle_longname" => $souscription[$profile_id]['long_name'],
																	   "pur_group" =>$bundle_group[$souscription[$profile_id]['type']],
																	   "pur_payment_type" =>$pt[$souscription[$profile_id]['pm']],
																	   "pur_amnt" => floatval($pur_amnt),
																	   "pur_amnt_ln" => $pur_amnt_ln);
													
														if($purchase_iter < 10000)  {
															$bulk->insert($document);
															$purchase_iter++;
														}
														else {
															$bulk->insert($document);
															$mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
															if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." purchase docs ". (time() - $t)."\n";
															$bulk = new MongoDB\Driver\BulkWrite();
															$purchase_iter = 0;
															$fact++;
														}                                                
													//$collection_purchase->insert($document);
												//}
                                           }
                                        break;
                                    }
                                break;
                            }
                        }
                    }
                }
            }
            if ($purchase_iter != 0 ) {
                echo "Inserting remaining purchase docs : ".$purchase_iter."\n";
                $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
            }
            echo 'nb rec:' . $record . "\n"; 


         
            // SDP
            tep_db_connect();
            $query = tep_db_query(  "SELECT ocs_event_date, ocs_calling_msisdn,  IF(ocs_amount='', ocs_value_before - ocs_value_after,ocs_amount) ocs_amount,  ocs_event_origin, bundle 
                                    FROM DWH.cdr_sdp c
                                    INNER JOIN DM_RF.rf_event_origin t2 on c.ocs_event_origin = t2.event_origin
                                    where ocs_event_date BETWEEN '" . $db ."' AND '" . $df ."' AND (ocs_value_before - ocs_value_after) >0 and rev_adj=1  -- and OCS_CALLING_MSISDN='261327148604'");
            $bulk = new MongoDB\Driver\BulkWrite();
            $purchase_iter = 0;
            $fact = 1;
            while ($res = tep_db_fetch_array($query)) {
                $msisdn = $res['ocs_calling_msisdn'];$profile_id = $res['ocs_event_origin'];
                if($res['bundle']==1) { // souscription
                    $cbm[$msisdn][3][$profile_id][$customer[$msisdn]['site_id']]['qty'] ++;
                    $cbm[$msisdn][3][$profile_id][$customer[$msisdn]['site_id']]['revenue'] += $res['ocs_amount'];
                }else { // VAS
                    $cbm[$msisdn][1][1][$customer[$msisdn]['site_id']]['voice_vas_cnt'] ++;
                    $cbm[$msisdn][1][1][$customer[$msisdn]['site_id']]['voice_vas_amnt'] += $res['ocs_amount'];
                    $cbm[$msisdn][1][1][$customer[$msisdn]['site_id']]['voice_vas_amnt1'] += $res['ocs_amount'];
                }
                $document = array(  "day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
                                    "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($res['ocs_event_date'])*1000 +$offset),
                                    "party_id" => strval($msisdn),
                                    "subs_id" => $customer[$msisdn]['subs_id'],
									"cust_code" => $customer[$msisdn]['cust_code'],
                                    "billing_type" => $btype[$customer[$msisdn]['billing_type']],
                                    "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
                                    "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
                                    "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
                                    "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
                                    "pur_name" => 'bundle',
                                    "pur_code" => $profile_id,
                                    "pur_bndle" => $souscription[$profile_id]['name'],
                                    "pur_bndle_longname" => $souscription[$profile_id]['long_name'],
                                    "pur_group" =>$bundle_group[$souscription[$profile_id]['type']],
                                    "pur_payment_type" =>$pt[$souscription[$profile_id]['pm']],
                                    "pur_amnt" => floatval($res['ocs_amount']));
                                    if($purchase_iter < 10000)  {
                                        $bulk->insert($document);
                                        $purchase_iter++;
                                    }
                                    else {
                                        $bulk->insert($document);
                                        $mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
                                        if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." SDP purchase docs ". (time() - $t)."\n";
                                        $bulk = new MongoDB\Driver\BulkWrite();
                                        $purchase_iter = 0;
                                        $fact++;
                                    }                 
                                    //$collection_purchase->insert($document);
            }
            if ($purchase_iter != 0 ) {
                echo "Inserting remaining SDP purchase docs : ".$purchase_iter."\n";
                $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
            }
            // RENOUVELLEMENT
            tep_db_connect();
            $query = tep_db_query(  "SELECT  ocs_event_date, ocs_calling_msisdn,ocs_amount, ocs_duration FROM DWH.cdr_sdp 
                        WHERE OCS_EVENT_DATE BETWEEN '" .$db . "' AND '" . $df . "' AND ocs_node_type ='SDP_PAM' AND ocs_record_type ='PAM_SCHEDULED'  -- and OCS_CALLING_MSISDN='261327148604'");	
            $bulk = new MongoDB\Driver\BulkWrite();
            $purchase_iter = 0;
            $fact = 1;		
            while ($res = tep_db_fetch_array($query)) {
                $msisdn = $res['ocs_calling_msisdn'];
                $sdp_pam = array();
                $sdp_pam1 = explode('+',$res['ocs_duration']);
                $sdp_pam2 = explode('+',$res['ocs_amount']);
                for ($i = 0; $i< sizeof($sdp_pam1); $i++){ 
                    if(($sdp_pam2[$i] <0) &&  ($bundle_renew[$sdp_pam1[$i]]==1)){
                        $profile_id = $sdp_pam1[$i];
                        $cbm[$msisdn][3][$profile_id][$customer[$msisdn]['site_id']]['qty'] ++;
                        $cbm[$msisdn][3][$profile_id][$customer[$msisdn]['site_id']]['revenue'] += abs($sdp_pam2[$i]);
                        $document = array(  "day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
                                            "purchase_date" => new MongoDB\BSON\UTCDateTime(strtotime($res['ocs_event_date'])*1000 +$offset),
                                            "party_id" => strval($msisdn),
                                            "subs_id" => $customer[$msisdn]['subs_id'],
											"cust_code" => $customer[$msisdn]['cust_code'],
                                            "billing_type" => $btype[$customer[$msisdn]['billing_type']],
                                            "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
                                            "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
                                            "loc_purchase" => isset($location[$customer[$msisdn]['site_id']]['name']) ? $location[$customer[$msisdn]['site_id']]['name']:$location[$customer[$msisdn]['loc_code']]['name'],
                                            "pp_name" =>$offer[$customer[$msisdn]['tmcode']]['name'],
                                            "pur_name" => 'bundle',
                                            "pur_code" => $profile_id,
                                            "pur_bndle" => $souscription[$profile_id]['name'],
                                            "pur_bndle_longname" => $souscription[$profile_id]['long_name'],
                                            "pur_group" =>$bundle_group[$souscription[$profile_id]['type']],
                                            "pur_payment_type" =>$pt[$souscription[$profile_id]['pm']],
                                            "pur_amnt" => floatval(abs($sdp_pam2[$i])));
                                            if($purchase_iter < 10000)  {
                                                $bulk->insert($document);
                                                $purchase_iter++;
                                            }
                                            else {
                                                $bulk->insert($document);
                                                $mng = $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
                                                if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." renouvellement purchase docs ". (time() - $t)."\n";
                                                $bulk = new MongoDB\Driver\BulkWrite();
                                                $purchase_iter = 0;
                                                $fact++;
                                            }       						
                    }
                }
            }
            if ($purchase_iter != 0 ) {
                echo "Inserting remaining renouvellement purchase docs : ".$purchase_iter."\n";
                $manager->executeBulkWrite('cbm.daily_purchase', $bulk, $writeConcern);
            }
			
            // INcomming  PROCESS
            tep_db_connect();
            $query = tep_db_query("SELECT msc_record_type, msc_duration, if(msc_record_type in(100,7), msc_served_msisdn, msc_called_msisdn) called, msc_calling_country,
                                msc_calling_dest
                                FROM DWH.cdr_unrated
                                WHERE msc_start_com BETWEEN '" . $db ."' AND '" . $df ."' AND msc_record_type in (1,100,7)
                                AND msc_served_imsi NOT LIKE '64602010%'  and msc_calling_dest <>1
                                AND msc_served_imsi LIKE '64602%' AND LENGTH(if(msc_record_type in(100,7), msc_served_msisdn, msc_called_msisdn)) >= 9
                                and substring(if(msc_record_type in(100,7), msc_served_msisdn, msc_called_msisdn),4,2) = '32' ");
            while ($res = tep_db_fetch_array($query)) {
                $cdr_type = $res['msc_record_type'];
                $cp1 = $res['msc_calling_country'];
                $msisdn = $res['called'];
                $duration = intval($res['msc_duration']);
                if($res['msc_calling_dest'] != '99') {
                    $op_id = $res['msc_calling_dest'];
                } else {
                    $op_id = 99;
                    $subclass = $cp1;
                }
                switch ($cdr_type) { //$cbm[$msisdn][1][1][$site_id]['vas_bndl_vol'] += $duration_free;
                    case '1': // voix MT
                    case '100': // voix MT fwd
                        if($duration >0) {
                            $cbm[$msisdn][1][$op_id][0]['voice_i_cnt'] ++;
                            $cbm[$msisdn][1][$op_id][0]['voice_i_vol'] += $duration;
                            if($op_id == '99') {
                                $cbm1[$msisdn][$subclass][0]['voice_i_cnt'] ++;
                                $cbm1[$msisdn][$subclass][0]['voice_i_vol'] += $duration;
                            }
                        }
                    break;

                    case '7': // sms
                        $cbm[$msisdn][1][$op_id][0]['sms_i_cnt'] ++;
                        if($op_id=='99') {
                            $cbm1[$msisdn][$subclass][0]['sms_i_cnt'] ++;
                        }
                    break;
                }
            }
            // end traffic entrant
            // traitement des imei
            $imei_list=array();$last_imei=array();
            $query = tep_db_query("SELECT msc_calling_msisdn, msc_served_imei
                                FROM DWH.cdr_unrated c
                                WHERE msc_start_com BETWEEN '" . $db ."' AND '" . $df ."' AND msc_record_type in (0) AND msc_served_imsi NOT LIKE '64602010%' AND msc_served_imsi LIKE '64602%' AND LENGTH(msc_calling_msisdn) >= 9 
                                and substring(msc_calling_msisdn,4,2) = '32'");
            while($res = tep_db_fetch_array($query)) {
                $msisdn = $res['msc_calling_msisdn'];
                if(trim($res['msc_served_imei']) != '')  {
                    $imei = $res['msc_served_imei'];
                    $imei_list[$msisdn][$imei] =1;
                    $last_imei[$msisdn] = $imei;
                }
            }
            // OM                                        
            tep_db_connect();
            $query = tep_db_query(" select 'o1' type ,tra_sender_msisdn, tra_amount, tra_service_charge, tra_service_type, tra_transaction_tag
                                    FROM DWH.om_transaction_tra_v2
                                    WHERE upd_dt BETWEEN '" . $db ."' AND '" . $df ."' 
                                    AND tra_status = 'TS'
                                    AND tra_sender_category = 'SUBS'
                                    union all
                                    select 'o2' type ,tra_receiver_msisdn, tra_amount, tra_service_charge, tra_service_type, tra_transaction_tag
                                    FROM DWH.om_transaction_tra_v2
                                    WHERE upd_dt BETWEEN '" . $db ."' AND '" . $df ."' 
                                    AND tra_status = 'TS'
                                    AND tra_receiver_category = 'SUBS'");
            while ($res = tep_db_fetch_array($query)) {
                $cdr_type = $res['type'];
                $msisdn = '261'.substr($res['tra_sender_msisdn'],1);
                $amount = intval($res['tra_amount']);
                $fees = intval($res['tra_service_charge']);
                $site_id =  $customer[$msisdn]['site_id'];
                $op_id = $res['tra_service_type'] . '|' . $res['tra_transaction_tag'];
                if($cdr_type == 'o1') {
                    $cbm[$msisdn][6][$op_id][$site_id]['qty'] ++;
                    $cbm[$msisdn][6][$op_id][$site_id]['montant'] += abs($amount);
                    $cbm[$msisdn][6][$op_id][$site_id]['revenue'] += abs($fees);
                } else {
                    $cbm[$msisdn][6][$op_id][$site_id]['r_qty'] ++;
                    $cbm[$msisdn][6][$op_id][$site_id]['r_montant'] += abs($amount);
                    $cbm[$msisdn][6][$op_id][$site_id]['revenue'] += abs($fees);
                }
            } 
            
			// Reclamations 
            $query = tep_db_query("SELECT concat('261',trim(recl_numero_appel)) ms ,trim(recl_type_id) type_id, trim(recl_s_type_id) s_type_id, recl_source FROM DWH.agora_reclamation_rec
                WHERE recl_date_creation = '".$d."';");

            while ($res = tep_db_fetch_array($query)) {
                //$recl_type = 
                $msisdn = $res['ms'] ;
                $cbm[$msisdn][8][$rf_recl_type[$res['type_id']]][$rf_recl_s_type[$res['s_type_id']]][$res['recl_source']]['qty']++;

            }

            // Qualification
            $query = tep_db_query("SELECT trim(appelant) ms , trim(qualifpirmaire1) qual_type, trim(qualifsecondaire1) qual_s_type,
                sens_appel from DWH.appel_traite_tanalahy_all where date BETWEEN '".$db."' and '".$df."'");

            while ($res = tep_db_fetch_array($query)) {
                //$msisdn = substr($res['ms'], 3);
                $cbm[$msisdn][10][$res['qual_type']][$res['qual_s_type']]['sens'] = $res['sens_appel'];
                $cbm[$msisdn][10][$res['qual_type']][$res['qual_s_type']]['qty']++;
            }

            // Data
            $query = tep_db_query("SELECT msisdn, volume_rat_2G USAGE_2G, volume_rat_3G USAGE_3G, volume_rat_LTE USAGE_4G,total_volume data_vol  , vol_Unknown Unknown, vol_Web Web, vol_P2P P2P, vol_Download Download, vol_News News, vol_Mail Mail, vol_DB DB, vol_Others Others, vol_Control Control, vol_Games Games, vol_Streaming Streaming, vol_Chat Chat, vol_VoIP VoIP, vol_MailOrange MailOrange, vol_VPN VPN, vol_ICMP_DNS ICMP_DNS, vol_MMS MMS, vol_MobileTV MobileTV, vol_OrangePortal OrangePortal, vol_roaming roaming  FROM DWH.otarie_heavy_user_appli WHERE upd_dt = '".$d."'");

            while ($res = tep_db_fetch_array($query)) {
               //$msisdn = substr($res['msisdn'], 3);
               //print_r($res);
               foreach ($res as $field => $value) {
                   //echo "field : ".$field."\n";
                   if($field == 'msisdn') continue;

                   if (floatval($value) > 0 ) $cbm[$msisdn][12][$field] += floatval($value);
               }
               
            }
			
			// Campagne 
            $query = tep_db_query("SELECT bonus_msisdn , bonus_cmp_id, bonus_type, bonus_category, bonus_validity, bonus_amount
                FROM DWH.lms_bonus where bonus_date BETWEEN '".$db."' and '".$df."'");

            while ($res = tep_db_fetch_array($query)) {
                //$msisdn = substr($res['bonus_msisdn'], 3);
                $cmp_id = $res['bonus_cmp_id'];
                $bonus_tpe = $res['bonus_type'];
                $bonus_cat = $bonus_cg[$res['bonus_category']];
				if ($bonus_tpe == 'VOIX_ORG_BNDL') $bonus_cat = 'amount';
				elseif ($bonus_tpe == 'bdl_sms_versorange') $bonus_cat = 'sms';
                $cbm[$msisdn][14][$cmp_id][$bonus_cat][$bonus_tpe]['bonus_validity'] = $res['bonus_validity'];
                $cbm[$msisdn][14][$cmp_id][$bonus_cat][$bonus_tpe]['bonus_vol'] += $res['bonus_amount'];
                $cbm[$msisdn][14][$cmp_id][$bonus_cat][$bonus_tpe]['qty']++;
            }

            //print_r($cbm);
            $bulk = new MongoDB\Driver\BulkWrite();
            $usage_iter = 0;
            $fact++;
            foreach ($cbm as $msisdn =>&$value) {
                $document = array(  "day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
                                    "subs_id" => $customer[$msisdn]['subs_id'],
                                    "party_id" => strval($msisdn),
									"cust_code" => $customer[$msisdn]['cust_code'],
                                    "billing_type" => $btype[$customer[$msisdn]['billing_type']],
                                    "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
                                    "pp_name" => $offer[$customer[$msisdn]['tmcode']]['name'],
                                    "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
                                    "last_imei" => $last_imei[$msisdn]);
                // traitement de la voix / data/ sms
                $document4 = array();
                $revenue_v_out=0;$revenue_s_out=0;$revenue_d_out=0;$revenue_m_out=0;$duration_out=0;$sms_out=0;$duration_in=0;$duration_out=0;$sms_in=0;$revenue_out=0;$revenue_in=0;$revenue_tot =0;$total_data=0;
                $voice_in=0;$voice_out=0;$revenue_vv_out1=0;$revenue_vv_out=0;$revenue_sv_out1=0;$revenue_sv_out=0;
                $revenue_v_out1=0;$revenue_s_out1=0;$revenue_d_out1=0;$revenue_m_out1=0;$revenue_out1=0;$revenue_tot1 =0;$revenue_v_ln=0;$revenue_s_ln=0;$revenue_d_ln=0;$revenue_v_ld=0;$revenue_s_ld=0;$revenue_d_ld=0;
                foreach ($value[1] as $op_id =>&$value2) { // operateur
                    $doc_operateur = array("op_code" => $operateur[$op_id]);
                    $document3=array();$usage_inc=array();
                    foreach ($value2 as $site_id =>&$value1) { // site
                        $doc_loc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);
                        $usage = array(); // on affiche que les valeurs non nulles
                        // incomming
                        if ($value1['voice_i_cnt'] >0)  $usage_inc['voice_i_cnt']  += $value1['voice_i_cnt'];
                        if ($value1['voice_i_vol'] >0)  $usage_inc['voice_i_vol']  += $value1['voice_i_vol'];
                        if ($value1['sms_i_cnt'] >0)    $usage_inc['sms_i_cnt']    += $value1['sms_i_cnt'];
                        if ($value1['voice_i_amnt'] >0) $usage_inc['voice_i_amnt'] += $value1['voice_i_amnt'];
                        // ougoing
                        if ($value1['voice_o_cnt'] + $value1['vas_cnt'] >0) $usage['voice_o_cnt'] = intval($value1['voice_o_cnt'] + $value1['vas_cnt']);
                        if ($value1['voice_o_main_vol'] >0) $usage['voice_o_main_vol'] = intval($value1['voice_o_main_vol']);
                        if ($value1['voice_o_amnt'] >0) $usage['voice_o_amnt'] = $value1['voice_o_amnt'];
                        if ($value1['voice_o_amnt1'] >0) $usage['voice_o_amnt1'] = $value1['voice_o_amnt1'];
                       
                        if ($value1['voice_o_bndl_vol'] >0) $usage['voice_o_bndl_vol'] = intval($value1['voice_o_bndl_vol']);
                        if ($value1['sms_o_main_cnt'] >0) $usage['sms_o_main_cnt'] = intval($value1['sms_o_main_cnt']);
                        if ($value1['sms_o_bndl_cnt'] >0) $usage['sms_o_bndl_cnt'] = intval($value1['sms_o_bndl_cnt']);
                        if ($value1['sms_o_amnt'] ) $usage['sms_o_amnt'] = $value1['sms_o_amnt'];
                        if ($value1['sms_o_amnt1'] ) $usage['sms_o_amnt1'] = $value1['sms_o_amnt1'];                        
                        if ($value1['data_main_vol'] >0) $usage['data_main_vol'] = $value1['data_main_vol'];
                        if ($value1['data_amnt'] >0) $usage['data_amnt'] = $value1['data_amnt'];
                        if ($value1['data_amnt1'] >0) $usage['data_amnt1'] = $value1['data_amnt1'];
                        // lany credit
                        if ($value1['voice_o_ln'] >0) $usage['voice_o_ln'] = $value1['voice_o_ln'];
						if ($value1['voice_o_ld'] >0) $usage['voice_o_ld'] = $value1['voice_o_ld'];
                        if ($value1['sms_o_ln'] ) $usage['sms_o_ln'] = $value1['sms_o_ln'];
						if ($value1['sms_o_ld'] ) $usage['sms_o_ld'] = $value1['sms_o_ld'];
                        if ($value1['data_ln'] >0) $usage['data_ln'] = $value1['data_ln'];
						if ($value1['data_ld'] >0) $usage['data_ld'] = $value1['data_ld'];
                        if ($value1['sms_vas_ln'] ) $usage['sms_vas_ln'] = $value1['sms_vas_ln'];
                        if ($value1['sms_vas_ld'] ) $usage['sms_vas_ld'] = $value1['sms_vas_ld'];
						if ($value1['voice_vas_ln'] ) $usage['voice_vas_ln'] = $value1['voice_vas_ln'];                        
                        if ($value1['voice_vas_ld'] ) $usage['voice_vas_ld'] = $value1['voice_vas_ld'];                        
						//
                        if ($value1['2G'] >0) $usage['usage_2G'] = $value1['2G'];
                        if ($value1['3G'] >0) $usage['usage_3G'] = $value1['3G'];
                        if ($value1['4G'] >0) $usage['usage_4G'] = $value1['4G'];
                        if ($value1['data_bndl_vol'] >0) $usage['data_bndl_vol'] = $value1['data_bndl_vol'];
                        if ($value1['voice_vas_cnt'] >0) $usage['voice_vas_cnt'] = intval($value1['voice_vas_cnt']);
                        if ($value1['voice_vas_amnt'] >0) $usage['voice_vas_amnt'] = intval($value1['voice_vas_amnt']);
                        if ($value1['voice_vas_amnt1'] >0) $usage['voice_vas_amnt1'] = intval($value1['voice_vas_amnt1']);
                        if ($value1['voice_vas_main_vol'] >0) $usage['voice_vas_main_vol'] = intval($value1['voice_vas_main_vol']);
                        if ($value1['voice_vas_bndl_vol'] >0) $usage['voice_vas_bndl_vol'] = intval($value1['voice_vas_bndl_vol']); 
                        if ($value1['sms_vas_cnt']) $usage['sms_vas_cnt'] = intval($value1['sms_vas_cnt']);
                        if ($value1['sms_vas_bndl_cnt']) $usage['sms_vas_bndl_cnt'] = intval($value1['sms_vas_bndl_cnt']);
                        if ($value1['sms_vas_amnt']) $usage['sms_vas_amnt'] = intval($value1['sms_vas_amnt']);
                        if ($value1['sms_vas_amnt1']) $usage['sms_vas_amnt1'] = intval($value1['sms_vas_amnt1']);                                               
                        $duration_out += $value1['voice_o_main_vol'] + $value1['voice_o_bndl_vol'] +$value1['voice_vas_main_vol'] +$value1['voice_vas_bndl_vol'];
                        $duration_in += $value1['voice_i_vol'];
                        $sms_out += $value1['sms_o_bndl_cnt']+ $value1['sms_vas_cnt'] + $value1['sms_o_main_cnt'] + $value1['sms_vas_bndl_cnt'] ;
                        $sms_in += $value1['sms_i_cnt'];
                        $voice_out +=  $value1['voice_o_cnt']  + $value1['voice_vas_cnt'] ;
                        $voice_in +=  $value1['voice_i_cnt'];
                        $revenue_v_out += $value1['voice_o_amnt'] + $value1['voice_vas_amnt'];
                        $revenue_s_out += $value1['sms_o_amnt'] + $value1['sms_vas_amnt'];
                        $revenue_d_out += $value1['data_amnt'];
                        $revenue_v_out1 += $value1['voice_o_amnt1'] + $value1['voice_vas_amnt1'] ;
                        $revenue_s_out1 += $value1['sms_o_amnt1'] + $value1['sms_vas_amnt1'];
                        $revenue_d_out1 += $value1['data_amnt1'];
                        // lanycredit
                        $revenue_v_ln += $value1['voice_o_ln'] + $value1['voice_vas_ln'];
                        $revenue_s_ln += $value1['sms_o_ln'] + $value1['sms_vas_ln'];    
                        $revenue_d_ln += $value1['data_ln']; 
                        //
                        $total_data += $value1['data_bndl_vol'] + $value1['data_main_vol'];
                        $revenue_tot += ($value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt'] +$value1['voice_vas_amnt'] + $value1['sms_vas_amnt'] +$value1['voice_o_ln'] + $value1['voice_vas_ln']+$value1['sms_o_ln'] + $value1['sms_vas_ln']+$value1['data_ln']);
                        $revenue_tot1 += ($value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1'] +$value1['voice_vas_amnt1'] + $value1['sms_vas_amnt1'] +$value1['voice_o_ln'] + $value1['voice_vas_ln']+$value1['sms_o_ln'] + $value1['sms_vas_ln']+$value1['data_ln']);
                        $revenue_out += ($value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt'] +$value1['voice_vas_amnt'] + $value1['sms_vas_amnt']+$value1['voice_o_ln'] + $value1['voice_vas_ln']+$value1['sms_o_ln'] + $value1['sms_vas_ln']+$value1['data_ln']);
                        $revenue_out1 += ($value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1'] +$value1['voice_vas_amnt1'] + $value1['sms_vas_amnt1']+$value1['voice_o_ln'] + $value1['voice_vas_ln']+$value1['sms_o_ln'] + $value1['sms_vas_ln']+$value1['data_ln']);
                        // traitement des da
                        $doca = array();$doc1 = array();
                        //$cbm_da[$msisdn][1][$site_id][$value['da_id']][3][$value['da_unit']]['vol']
                        foreach($cbm_da[$msisdn][$op_id][$site_id] as $daid => &$valueda1) {
                            if($daid>0) {
                                foreach($valueda1 as $evt => &$valueda2) { 
										foreach($valueda2 as $daoffer => &$valueda3) {
											foreach($valueda3 as $unit => &$valueda) {
												$doc_da = array("da_id" => $daid, "da_name" =>  $datype[$daid][$daoffer]['name'], "da_offer" => $daoffer, "da_category" =>  $datype[$daid][$daoffer]['category'], "da_unit"=>$daunit[$unit], "da_type" =>  $datype[$daid][$daoffer]['type']);
												$docda1 =array("da_usage" => $type_event[$evt], "da_cnt" => intval($valueda['cnt']), "da_qty" => $valueda['qty'], "da_vol" => intval($valueda['vol']));
												if(!empty($docda1)){ 
													$doca[] = array_merge($doc_da,$docda1); 
												} 
											}
                                    }                                    
                                }                                
                            }
                        }
                        if (!empty($doca)) {
                            $doc1 = array("da"=>$doca);
                        }
                        if (!empty($usage)) {
                            $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);	
                            $document2 = array_merge($doc, $usage, $doc1);
                            $document3[] = $document2;	
                        }
                    } // site
                    if ((!empty($document3)) || (!empty($usage_inc))) {
                        $doc = array("usage_op" => $document3);
                        if(empty($document3)) $doc=array();
                        if(!empty($usage_inc)) {
                            $document2 = array_merge( $doc_operateur, $usage_inc, $doc);
                        }else {
                            $document2 = array_merge( $doc_operateur, $doc);
                        }
                        $document4[] = $document2;
                    }
                } // operateur
                if (!empty($document4)) {
                        $doc = array("usage" => $document4);
                        $document = array_merge($document, $doc);
                }
                // roaming
                // traitement du roaming
                //print_r($cbmr_da);
                $document1 = array();
                foreach ($cbm_roaming[$msisdn] as $mcc =>&$value2) { // mcc $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_cnt']
                    foreach ($value2 as $op_id =>&$value1) { // op_id
                        $usage = array(); // on affiche que les valeurs non nulles
                        $doc1 = array("roaming_mcc" => substr($mcc,0,3), "roaming_country" => $mcc_country[$mcc]['country'],"network_code" => substr($mcc,3,2), "network_name" => $mcc_country[$mcc]['network'], "op_code" => $operateur[$op_id]);
                        // ougoing
                        if ($value1['voice_o_cnt'] >0) $usage['voice_o_cnt'] = $value1['voice_o_cnt'];
                        if ($value1['voice_o_main_vol'] >0) $usage['voice_o_main_vol'] = $value1['voice_o_main_vol'];
                        if ($value1['voice_o_amnt'] >0) $usage['voice_o_amnt'] = $value1['voice_o_amnt'];
                        if ($value1['voice_o_amnt1'] >0) $usage['voice_o_amnt1'] = $value1['voice_o_amnt1'];
                        if ($value1['voice_o_bndl_vol'] >0) $usage['voice_o_bndl_vol'] = $value1['voice_o_bndl_vol'];
                        if ($value1['sms_o_main_cnt'] >0) $usage['sms_o_main_cnt'] = $value1['sms_o_main_cnt'];
                        if ($value1['sms_o_bndl_cnt'] >0) $usage['sms_o_bndl_cnt'] = $value1['sms_o_bndl_cnt'];
                        if ($value1['sms_o_amnt'] >0) $usage['sms_o_amnt'] = $value1['sms_o_amnt'];
                        if ($value1['sms_o_amnt1'] >0) $usage['sms_o_amnt1'] = $value1['sms_o_amnt1'];
                        if ($value1['data_main_vol'] >0) $usage['data_main_vol'] = $value1['data_main_vol'];
                        if ($value1['data_amnt'] >0) $usage['data_amnt'] = $value1['data_amnt'];
                        if ($value1['data_amnt1'] >0) $usage['data_amnt1'] = $value1['data_amnt1'];
                        if ($value1['data_bndl_vol'] >0) $usage['data_bndl_vol'] = $value1['data_bndl_vol'];
                        // lanycredit
                        if ($value1['voice_o_ln'] >0) $usage['voice_o_ln'] = $value1['voice_o_ln'];
                        if ($value1['sms_o_ln'] ) $usage['sms_o_ln'] = $value1['sms_o_ln'];
                        if ($value1['data_ln'] >0) $usage['data_ln'] = $value1['data_ln'];
						// lanydata
                        if ($value1['voice_o_ld'] >0) $usage['voice_o_ld'] = $value1['voice_o_ld'];
                        if ($value1['sms_o_ld'] ) $usage['sms_o_ld'] = $value1['sms_o_ld'];
                        if ($value1['data_ld'] >0) $usage['data_ld'] = $value1['data_ld'];
                        //
                        $duration_out += $value1['voice_o_main_vol'] + $value1['voice_o_bndl_vol'];
                        $sms_out += $value1['sms_o_main_cnt'] + $value1['sms_o_bndl_cnt'] ;
                        $voice_out +=  $value1['voice_o_cnt'] ;
                        $revenue_v_out += $value1['voice_o_amnt'];
                        $revenue_v_out1 += $value1['voice_o_amnt1'];
                        $revenue_s_out += $value1['sms_o_amnt'];
                        $revenue_s_out1 += $value1['sms_o_amnt1'];
                        $revenue_d_out += $value1['data_amnt'];
                        $revenue_d_out1 += $value1['data_amnt1'];                        
                        // lanycredit
                        $revenue_v_ln += $value1['voice_o_ln'] + $value1['voice_vas_ln'];
                        $revenue_s_ln += $value1['sms_o_ln'] + $value1['sms_vas_ln'];    
                        $revenue_d_ln += $value1['data_ln']; 
                        //                       
                        $revenue_tot += $value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt'] +  $value1['voice_vas_ln']+$value1['sms_vas_ln']+$value1['data_ln'];
                        $revenue_tot1 += $value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1'] +  $value1['voice_vas_ln']+$value1['sms_vas_ln']+$value1['data_ln'];
                        $revenue_out += $value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt']  +  $value1['voice_vas_ln']+$value1['sms_vas_ln']+$value1['data_ln'];
                        $revenue_out1 += $value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1']  +  $value1['voice_vas_ln']+$value1['sms_vas_ln']+$value1['data_ln'];
                        $total_data += $value1['data_o_bndl_vol'] + $value1['data_o_main_vol'];
                        // traitement des da
                        $doca = array();$doc2 = array();
                        foreach($cbmr_da[$msisdn][$mcc][$op_id] as $daid => &$valueda1) {
                            $docda1 =array();
                            if($daid>0) {
                                foreach($valueda1 as $evt => &$valueda2) { 
									foreach($valueda2 as $daoffer => &$valueda3) {
										foreach($valueda3 as $unit => &$valueda) {
											$doc_da = array("da_id" => $daid, "da_name" =>  $datype[$daid][$daoffer]['name'], "da_offer" => $daoffer, "da_category" =>  $datype[$daid][$daoffer]['category'], "da_unit"=>$daunit[$unit], "da_type" =>  $datype[$daid][$daoffer]['type']);
											$docda1 =array("da_usage" => $type_event[$evt], "da_cnt" => intval($valueda['cnt']), "da_qty" => $valueda['qty'], "da_vol" => intval($valueda['vol']));
											if(!empty($docda1)){ 
												$doca[] = array_merge($doc_da,$docda1); 
											} 
										}   
									}		
                                }                                
                            }
                        }
                        if (!empty($doca)) {
                            $doc2 = array("da"=>$doca);
                        }
                        if(!empty($usage)) {
                            $document2 = array_merge($doc1, $usage, $doc2);
                            $document1[] = $document2;	
                        }
                    }
                }
                if (!empty($document1)) {
                    $doc = array("roaming" => $document1);
                    $document = array_merge($document, $doc);
                }
                // traitement des recharges
                $document3=array();$topup_tbonus;$topup_tqty=0;$topup_tmnt=0;$total_ec_fees = 0;$total_ec_payback = 0;$p2p_tmnt=0;$p2p_tqty=0;
                foreach ($value[2] as $rec =>&$value2) { // type de recharge
                    $doc_rec = array("rec_type" => $recharge[$rec], "rec_code" => $rec); 
                    $document1 = array();
                    foreach ($value2 as $site_id =>&$value1) { // site
                        $usage = array(); // on affiche que les valeurs non nulles
                        if ($value1['qty'] >0) $usage['rec_cnt'] = $value1['qty'];
                        if ($value1['revenue'] >0) $usage['rec_amnt'] = $value1['revenue'];
						if($rec !='OK') {
							$topup_tqty += $value1['qty'];
							$topup_tmnt += $value1['revenue'];
						}else { // P2P
							$p2p_tqty += $value1['qty'];
							$p2p_tmnt += $value1['revenue'];							
						}
                        if(!empty($usage)) {
                            $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);	
                            $document2 = array_merge($doc, $usage);
                            $document1[] = $document2;	
                        }
                    } // site _id
                    if(!empty($document1)) {
                        $doc = array("recharge" => $document1);
                        $document2  = array_merge($doc_rec, $doc);
                        $document3[] = $document2;
                    }
                }
                if (!empty($document3)) {
                    $doc = array("topup" => $document3);
                    $document = array_merge($document, $doc);
                }
                // traitement des EC
                $document1=array();$total_ec_fees = 0;$total_ec_payback = 0;$total_ec_loan = 0;$total_ec_qty = 0; $total_ca_reactivation=0;//$cbm[$msisdn][9][1][$site_id][0]['ec_fees']
				$total_ed_fees = 0;$total_ed_payback = 0;$total_ed_loan = 0;$total_ed_qty = 0;
                foreach($value[9][1] as $site_id =>&$value1) { // site
                    $usage = array(); 
                    if ($value1['ec_loan'] >0) $usage['ec_loan'] = $value1['ec_loan'];
                    if ($value1['ec_qty'] >0) $usage['ec_qty'] = $value1['ec_qty'];
                    if ($value1['ec_fees'] >0) $usage['ec_fees'] = $value1['ec_fees'];
                    if ($value1['ec_payback'] >0) $usage['ec_payback'] = $value1['ec_payback'];
					if ($value1['ed_loan'] >0) $usage['ed_loan'] = $value1['ed_loan'];
                    if ($value1['ed_qty'] >0) $usage['ed_qty'] = $value1['ed_qty'];
                    if ($value1['ed_fees'] >0) $usage['ed_fees'] = $value1['ed_fees'];
                    if ($value1['ed_payback'] >0) $usage['ed_payback'] = $value1['ed_payback'];
                    if ($value1['ca_reactivation'] >0) $usage['ca_reactivation'] = $value1['ca_reactivation'];
                    $total_ec_loan += $value1['ec_loan'];
                    $total_ec_qty += $value1['ec_qty'];
                    $total_ec_fees += $value1['ec_fees'];
                    $total_ec_payback += $value1['ec_payback'];
					$total_ed_loan += $value1['ed_loan'];
                    $total_ed_qty += $value1['ed_qty'];
                    $total_ed_fees += $value1['ed_fees'];
                    $total_ed_payback += $value1['ed_payback'];
                    $total_ca_reactivation += $value1['ca_reactivation'];
                    $revenue_out += $value1['ec_fees']+$value1['ed_fees']+ $value1['ca_reactivation'];
                    $revenue_tot += $value1['ec_fees']+$value1['ed_fees']+ $value1['ca_reactivation'];
                    $revenue_out1 += $value1['ec_fees']+$value1['ed_fees']+ $value1['ca_reactivation'];
                    $revenue_tot1 += $value1['ec_fees']+$value1['ed_fees']+ $value1['ca_reactivation'];                    
                    if(!empty($usage)) {
                        $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);
                        $document2 = array_merge($doc, $usage);
                        $document1[] = $document2;	
                    }
                }
                if (!empty($document1)) {
                    $doc = array("EC" => $document1);
                    $document = array_merge($document, $doc);
                }
                // traitement des subscriptions
                $document3=array();$subs_tqty=0;$subs_tmnt=0;
                foreach ($value[3] as $rec =>&$value2) { // type de SUBSCRIPTION
                    $doc_rec = array( "bndle_code" => $rec,"bndle_name" => $souscription[$rec]['name'],"bndle_group" => $bundle_group[$souscription[$rec]['type']],"bndle_longname" => $souscription[$rec]['long_name']); 
                    $document1 = array();
                    foreach ($value2 as $site_id =>&$value1) { // site
                        $usage = array(); // on affiche que les valeurs non nulles
                        if ($value1['qty'] >0) $usage['bndle_cnt'] = $value1['qty'];
                        if ($value1['revenue'] >0) $usage['bndle_amnt'] = $value1['revenue'];
                        $subs_tqty += $value1['qty'];
                        $subs_tmnt += $value1['revenue'];
                        $revenue_out += $value1['revenue'];
                        $revenue_tot +=$value1['revenue'];
                        $revenue_out1 += $value1['revenue'];
                        $revenue_tot1 +=$value1['revenue'];
                        if(!empty($usage)) {
                            $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);	
                            $document2 = array_merge($doc, $usage);
                            $document1[] = $document2;	
                        }
                    } // site _id
                    if(!empty($document1)) {
                        $doc = array("subscription" => $document1);
                        $document2  = array_merge($doc_rec, $doc);
                        $document3[] = $document2;
                    }
                }
                if (!empty($document3)) {
                    $doc = array("bundle" => $document3);
                    $document = array_merge($document, $doc);
                }
                // traitement des IMEI
                $im =array();
                if($imei_list[$msisdn] !='') {
                    foreach($imei_list[$msisdn] as $imei =>$value1) $im[] = strval($imei); 
                    $doc = array("imei" => $im);
                    $document = array_merge($document, $doc);
                }
                // liste region //$region_list[$msisdn][$region_id] =1;
                $im =array();
                if(!empty($region_list[$msisdn])) {
                    foreach($region_list[$msisdn] as $region =>$value1) $im[] = $region;
                    $doc = array("zone_orange" => $im);
                    $document = array_merge($document, $doc);
                }
                // traitement  om
                $document3=array();$om_tqty=0;$om_tmnt=0;$om_tr_amnt=0;$om_trqty=0;$om_tr_ramnt=0;
                foreach ($value[6] as $rec =>&$value2) { // type de SUBSCRIPTION
                    $doc_rec = array("transaction_type" => $rec); 
                    $document1 = array();
                    foreach ($value2 as $site_id =>&$value1) { // site
                        $usage = array(); // on affiche que les valeurs non nulles
                        if ($value1['qty'] >0) $usage['om_cnt'] = $value1['qty'];
                        if ($value1['revenue'] >0) $usage['om_amnt'] = $value1['revenue'];
                        if ($value1['montant'] >0) $usage['om_tr_amnt'] = $value1['montant'];
                        if ($value1['r_qty'] >0) $usage['om_r_cnt'] = $value1['r_qty'];
                        if ($value1['r_montant'] >0) $usage['om_r_tr_amnt'] = $value1['r_montant'];
                        $om_tqty += $value1['qty'];
                        $om_tmnt += $value1['revenue'];
                        $om_tr_amnt += $value1['montant'];
                        $om_trqty += $value1['r_qty'];
                        $om_tr_ramnt += $value1['r_montant'];
                        $revenue_tot +=$value1['revenue'];
                        $revenue_tot1 +=$value1['revenue'];
                        if(!empty($usage)) {
                            $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);	
                            $document2 = array_merge($doc, $usage);
                            $document1[] = $document2;	
                        }
                    } // site _id
                    if(!empty($document1)) {
                        $doc = array("transaction" => $document1);
                        $document2  = array_merge($doc_rec, $doc);
                        $document3[] = $document2;
                    }
                }
                if (!empty($document3)) {
                    $doc = array("om" => $document3);
                    $document = array_merge($document, $doc);
                }
				
				// Traitement reclamation 
                //$cbm[$msisdn][8][$rf_recl_type[$res['type_id']]][$rf_recl_s_type[$res['s_type_id']]][$res['recl_source']]['qty']++;
                $document3 = array();
                $req_tot = 0;
                foreach ($value[8] as $recl_type => $valeur) {
                    foreach ($valeur as $recl_s_type => $value1) {
                        foreach ($value1 as $recl_source => $value2) {
                            $doc = array('req_type' => $recl_type, 'req_subtype' => $recl_s_type , 'req_source' => $recl_source,
                            'req_cnt' => $value2['qty'] );
                            $req_tot +=  $value2['qty'];
                            $document3[] = $doc;

                        }
                    }
                }
                if (!empty($document3)) {
                    $doc = array("request" => $document3);
                    $document = array_merge($document, $doc);
                }


                // Traitement Qualification 
                //$cbm[$msisdn][10][$res['qual_type']][$res['qual_s_type']]['qty']

                 $document3 = array();
                 $quali_tot_cnt = 0;
                 foreach ($value[10] as $qual_type => $valeur) {
                    foreach ($valeur as $qual_s_type => $value2) {
                        $doc = array('quali_type' => $qual_type, 'quali_subtype' => $qual_s_type, 'quali_sens_appel' => $value2['sens'],
                        'quali_cnt' => $value2['qty'] );
                        $quali_tot_cnt += $value2['qty'];
                        $document3[] = $doc;
                    }
                 }

                if (!empty($document3)) {
                    $doc = array("qualification" => $document3);
                    $document = array_merge($document, $doc);
                }

                // Traitement Data
                // $cbm[$msisdn][12][$field]

                $document3 = array();
                $data_vol_tot = 0;
                if(isset($value[12]['USAGE_2G'])) $document3['USAGE_2G'] = $value[12]['USAGE_2G'];
                if(isset($value[12]['USAGE_3G'])) $document3['USAGE_3G'] = $value[12]['USAGE_3G'];
                if(isset($value[12]['USAGE_4G'])) $document3['USAGE_4G'] = $value[12]['USAGE_4G'];
                if(isset($value[12]['data_vol'])) {
                    $document3['data_vol'] = $value[12]['data_vol'];
                    $data_vol_tot = $value[12]['data_vol'];
                }
                $data_type = array();
                foreach ($value[12] as $datatype => $value2) {
                    if($datatype == 'USAGE_2G' || $datatype == 'USAGE_3G' || $datatype == 'USAGE_4G' || $datatype == 'data_vol') continue;
                    $doc = array("data_type" => $datatype, "data_vol" => $value2);
                    $data_vol_tot += $value2;
                    $data_type[] = $doc;
                }
                if(!empty($data_type)) $document3['datatype'] = $data_type;
                if (!empty($document3)) {
                    $doc = array("data" => $document3);
                    $document = array_merge($document, $doc);
                }

                //Traitement Compagne 
                //$cbm[$msisdn][14][$cmp_id][$bonus_cat][$bonus_type]['bonus_validity'] = $res['bonus_validity'];
                $document3 = array();
                $bonus_tot_amnt = 0;
                $bonus_tot_cnt = 0;
                foreach ($value[14] as $cmp_id => $valeur) {
                    $camp_doc = array("bonus_cpg_id" => $cmp_id, "bonus_cpg_name" => $rf_campagne[$cmp_id]);
                    $bonus_camp_doc = array();
                    foreach ($valeur as $bonus_cat => $value1) {
                        foreach ($value1 as $bonus_tpe => $value2) {
                            $doc = array('bonus_type' => $bonus_tpe, 'bonus_cat' => $bonus_cat, 'bonus_vol' => $value2['bonus_vol'],
                                'bonus_qty' => $value2['qty'], 'bonus_validity' => $value2['bonus_validity']);
                            $bonus_camp_doc[] = $doc;
                            $bonus_tot_amnt += $value2['bonus_vol'];
                            $bonus_tot_cnt += $value2['qty'];
                        }
                    }
                    if(!empty($bonus_camp_doc)) $camp_doc = array_merge($camp_doc, ['bonus_campaign' => $bonus_camp_doc]);
                    $document3[] = $camp_doc;

                }
                if (!empty($document3)) {
                    $doc = array("campaign" => $document3);
                    $document = array_merge($document, $doc);
                } 
				
                // total
                $total=array();
                if($duration_out >0) $total['voice_o_tot_vol'] = $duration_out;
                if($voice_out >0) $total['voice_o_tot_cnt'] = $voice_out;
                if($duration_in >0) $total['voice_i_tot_vol'] = $duration_in;
                if($voice_in >0) $total['voice_i_tot_cnt'] = $voice_in;
                if($sms_in >0)$total['sms_i_tot_vol'] = $sms_in;
                if($sms_in >0)$total['sms_i_tot_cnt'] = $sms_in;
                if($revenue_in >0)$total['revenue_i_tot_amnt'] = $revenue_in;
                if($sms_out >0)$total['sms_o_tot_vol'] = $sms_out;
                if($sms_out >0)$total['sms_o_tot_cnt'] = $sms_out;
                if($revenue_v_out >0)$total['rev_v_o_main_amnt'] = $revenue_v_out;
                if($revenue_s_out >0)$total['rev_s_o_main_amnt'] = $revenue_s_out;
                if($revenue_d_out >0)$total['rev_d_main_amnt'] = $revenue_d_out;
                if($revenue_m_out >0)$total['rev_m_o_main_amnt'] = $revenue_m_out;
                if($revenue_v_out1 >0)$total['rev_v_o_main_amnt1'] = $revenue_v_out1;
                if($revenue_s_out1 >0)$total['rev_s_o_main_amnt1'] = $revenue_s_out1;
                if($revenue_d_out1 >0)$total['rev_d_main_amnt1'] = $revenue_d_out1;
                if($revenue_m_out1 >0)$total['rev_m_o_main_amnt1'] = $revenue_m_out1;
                // lanycredit
                if($revenue_v_ln >0) $total['rev_v_ln'] = $revenue_v_ln;
                if($revenue_s_ln >0) $total['rev_s_ln'] = $revenue_s_ln; 
                if($revenue_d_ln >0) $total['revenue_d_ln'] = $revenue_d_ln;  			
                //
                if($revenue_out >0)$total['rev_o_tot_amnt'] = $revenue_out;
                if($revenue_out1>0)$total['rev_o_tot_amnt1'] = $revenue_out1;
                if($subs_tqty >0)$total['bndle_tot_cnt'] = $subs_tqty;
                if($subs_tmnt >0)$total['bndle_tot_amnt'] = $subs_tmnt;
                if($topup_tqty >0)$total['rec_tot_cnt'] = $topup_tqty;
                if($topup_tmnt >0)$total['rec_tot_amnt'] = $topup_tmnt;
                if($p2p_tqty >0)$total['p2p_tot_cnt'] = $p2p_tqty;
                if($p2p_tmnt >0)$total['p2p_tot_amnt'] = $p2p_tmnt;
                if($revenue_tot >0) $total['rev_tot_amnt'] = $revenue_tot;
                if($revenue_tot1 >0)$total['rev_tot_amnt1'] = $revenue_tot1;
                if($total_data >0)$total['data_o_total_vol'] = $total_data;
                if($total_ec_loan >0)$total['ec_loan_tot_amnt'] = $total_ec_loan;
                if($total_ec_qty >0)$total['ec_loan_tot_cnt'] = $total_ec_qty;
                if($total_ec_fees >0)$total['ec_fees_tot_amnt'] = $total_ec_fees;
                if($total_ec_payback >0)$total['ec_payback_tot_amnt'] = $total_ec_payback;
				if($total_ed_loan >0)$total['ed_loan_tot_amnt'] = $total_ed_loan;
                if($total_ed_qty >0)$total['ed_loan_tot_cnt'] = $total_ed_qty;
                if($total_ed_fees >0)$total['ed_fees_tot_amnt'] = $total_ed_fees;
                if($total_ed_payback >0)$total['ed_payback_tot_amnt'] = $total_ed_payback;
                if($total_transfer_fees >0)$total['transfer_fees_tot_amnt'] = $total_transfer_fees;
                if($total_ca_reactivation >0)$total['ca_reactivation'] = $total_ca_reactivation;
                if($om_tmnt >0)$total['om_tot_amnt'] = $om_tmnt;
                if($om_tqty >0)$total['om_cnt'] = $om_tqty;
                if($om_tr_amnt >0)$total['om_tot_tr_amnt'] = $om_tr_amnt;
                if($om_trqty >0)$total['om_r_cnt'] = $om_trqty;
                if($om_tr_ramnt >0)$total['om_tot_r_tr_amnt'] = $om_tr_ramnt;
				if (($req_tot) > 0) $total['req_cnt'] = $req_tot;
                if (($quali_tot_cnt) > 0) $total['quali_cnt'] = $quali_tot_cnt;
                //if (($bonus_tot_amnt) > 0) $total['bonus_amnt'] = $bonus_tot_amnt;
                if (($bonus_tot_cnt) > 0) $total['bonus_cnt'] = $bonus_tot_cnt;     
				
                if(!empty($total)) {
                      $doc = array("total" => $total);
                      $document = array_merge($document, $doc);
                }
                // insertion

                if($usage_iter < 10000)  {
                    $bulk->insert($document);
                    $usage_iter++;
                }
                else {
                    $bulk->insert($document);
                    $mng = $manager->executeBulkWrite('cbm.daily_usage', $bulk, $writeConcern);
                    if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." usage docs ". (time() - $t)."\n";
                    $bulk = new MongoDB\Driver\BulkWrite();
                    $usage_iter = 0;
                    $fact++;
                }               
                //$collection->insert($document);
            }
            if ($usage_iter != 0 ) {
                echo "Inserting remaining usage docs : ".$usage_iter."\n";
                $manager->executeBulkWrite('cbm.daily_usage', $bulk, $writeConcern);
            }

            $bulk = new MongoDB\Driver\BulkWrite();
            $usage_iter = 0;
            $fact = 1;
            foreach ($cbm1 as $msisdn =>&$value) {
                $document = array("day" => new MongoDB\BSON\UTCDateTime($mgdate*1000 +$offset),
                                "subs_id" => $customer[$msisdn]['subs_id'],
                                "party_id" => strval($msisdn),
                                "cust_code" => $customer[$msisdn]['cust_code'],
								"billing_type" => $btype[$customer[$msisdn]['billing_type']],
                                "market" => $custgroup[$customer[$msisdn]['prg_code']]['market'],
                                "pp_name" => $offer[$customer[$msisdn]['tmcode']]['name'],
                                "loc_name" => $location[$customer[$msisdn]['loc_code']]['name'],
                                "last_imei" => $last_imei[$msisdn]);
                // traitement de la voix / data/ sms
                $document4 = array();
                $revenue_v_out=0;$revenue_s_out=0;$revenue_d_out=0;$revenue_m_out=0;$duration_out=0;$sms_out=0;$duration_in=0;$duration_out=0;$sms_in=0;$revenue_out=0;$revenue_in=0;$revenue_tot =0;$total_data=0;
                $voice_in=0;$voice_out=0;
                $revenue_v_out1=0;$revenue_s_out1=0;$revenue_d_out1=0;$revenue_m_out1=0;$revenue_out1=0;$revenue_tot1 =0;$revenue_v_ln=0;$revenue_s_ln=0;$revenue_d_ln=0;
                foreach ($value as $op_id =>&$value2) { // operateur
                    if($op_id !=9) {
                        $doc_operateur = array("country_name" =>  $country[$op_id], "country_code" => $op_id);			  
                        $document3=array();$usage_inc=array();
                        foreach ($value2 as $site_id =>&$value1) { // site
                            $doc_loc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);
                            $usage = array(); // on affiche que les valeurs non nulles
                            // incomming
                            if ($value1['voice_i_cnt'] >0)  $usage_inc['voice_i_cnt']  += $value1['voice_i_cnt'];
                            if ($value1['voice_i_vol'] >0)  $usage_inc['voice_i_vol']  += $value1['voice_i_vol'];
                            if ($value1['sms_i_cnt'] >0)    $usage_inc['sms_i_cnt']    += $value1['sms_i_cnt'];
                            if ($value1['voice_i_amnt'] >0) $usage_inc['voice_i_amnt'] += $value1['voice_i_amnt'];
                            // ougoing
                            if ($value1['voice_o_cnt'] + $value1['vas_cnt'] >0) $usage['voice_o_cnt'] = intval($value1['voice_o_cnt'] + $value1['vas_cnt']);
                            if ($value1['voice_o_main_vol'] >0) $usage['voice_o_main_vol'] = intval($value1['voice_o_main_vol']);
                            if ($value1['voice_o_amnt'] >0) $usage['voice_o_amnt'] = $value1['voice_o_amnt'];
                            if ($value1['voice_o_amnt1'] >0) $usage['voice_o_amnt1'] = $value1['voice_o_amnt1'];
                            if ($value1['voice_o_bndl_vol'] >0) $usage['voice_o_bndl_vol'] = intval($value1['voice_o_bndl_vol']);
                            if ($value1['sms_o_main_cnt'] >0) $usage['sms_o_main_cnt'] = intval($value1['sms_o_main_cnt']);
                            if ($value1['sms_o_bndl_cnt'] >0) $usage['sms_o_bndl_cnt'] = intval($value1['sms_o_bndl_cnt']);
                            if ($value1['sms_o_amnt'] ) $usage['sms_o_amnt'] = $value1['sms_o_amnt'];
                            if ($value1['sms_o_amnt1'] ) $usage['sms_o_amnt1'] = $value1['sms_o_amnt1'];
                            if ($value1['voice_vas_cnt'] >0) $usage['voice_vas_cnt'] = intval($value1['voice_vas_cnt']);
                            if ($value1['voice_vas_amnt'] >0) $usage['voice_vas_amnt'] = intval($value1['voice_vas_amnt']);
                            if ($value1['voice_vas_amnt1'] >0) $usage['voice_vas_amnt1'] = intval($value1['voice_vas_amnt1']);
                            if ($value1['voice_vas_main_vol'] >0) $usage['voice_vas_main_vol'] = intval($value1['voice_vas_main_vol']);
                            if ($value1['voice_vas_bndl_vol'] >0) $usage['voice_vas_bndl_vol'] = intval($value1['voice_vas_bndl_vol']); 
                            // lany credit
                            if ($value1['voice_o_ln'] >0) $usage['voice_o_ln'] = $value1['voice_o_ln'];
                            if ($value1['sms_o_ln'] ) $usage['sms_o_ln'] = $value1['sms_o_ln'];
                            //
                            if ($value1['sms_vas_cnt']) $usage['sms_vas_cnt'] = intval($value1['sms_vas_cnt']);
                            if ($value1['sms_vas_bndl_cnt']) $usage['sms_vas_bndl_cnt'] = intval($value1['sms_vas_bndl_cnt']);
                            if ($value1['sms_vas_amnt']) $usage['sms_vas_amnt'] = intval($value1['sms_vas_amnt']);
                            if ($value1['sms_vas_amnt1']) $usage['sms_vas_amnt1'] = intval($value1['sms_vas_amnt1']);                                               
                            $duration_out += $value1['voice_o_main_vol'] + $value1['voice_o_bndl_vol'] +$value1['voice_vas_main_vol'] +$value1['voice_vas_bndl_vol'];
                            $duration_in += $value1['voice_i_vol'];
                            $sms_out += $value1['sms_o_bndl_cnt']+ $value1['sms_vas_cnt'] + $value1['sms_o_main_cnt'] + $value1['sms_vas_bndl_cnt'] ;
                            $sms_in += $value1['sms_i_cnt'];
                            $voice_out +=  $value1['voice_o_cnt']  + $value1['voice_vas_cnt'] ;
                            $voice_in +=  $value1['voice_i_cnt'];
                            $revenue_v_out += $value1['voice_o_amnt'] + $value1['voice_vas_amnt'];
                            $revenue_s_out += $value1['sms_o_amnt'] + $value1['sms_vas_amnt'];
                            $revenue_v_out1 += $value1['voice_o_amnt1'] + $value1['voice_vas_amnt1'];
                            $revenue_s_out1 += $value1['sms_o_amnt1']+  + $value1['sms_vas_amnt1'];
                            // lanycredit
                            $revenue_v_ln += $value1['voice_o_ln'];
                            $revenue_s_ln += $value1['sms_o_ln'];    
							
                            $revenue_tot += ($value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['voice_vas_amnt'] + $value1['sms_vas_amnt']+$value1['voice_o_ln']+$value1['sms_o_ln']);
                            $revenue_tot1 += ($value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['voice_vas_amnt1'] + $value1['sms_vas_amnt1']+$value1['voice_o_ln']+$value1['sms_o_ln']);
                            $revenue_out += ($value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['voice_vas_amnt'] + $value1['sms_vas_amnt']+$value1['voice_o_ln']+$value1['sms_o_ln']);
                            $revenue_out1 += ($value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['voice_vas_amnt1'] + $value1['sms_vas_amnt1']+$value1['voice_o_ln']+$value1['sms_o_ln']);
                            // traitement des da
                            $doca = array();$doc1 = array();
                            foreach($cbm1_da[$msisdn][$op_id][$site_id] as $daid => &$valueda1) {
                                if($daid>0) {
                                    foreach($valueda1 as $evt => &$valueda2) { 
										foreach($valueda2 as $daoffer => &$valueda3) {
											foreach($valueda3 as $unit => &$valueda) {
												$doc_da = array("da_id" => $daid, "da_name" =>  $datype[$daid][$daoffer]['name'], "da_offer" => $daoffer, "da_category" =>  $datype[$daid][$daoffer]['category'], "da_unit"=>$daunit[$unit], "da_type" =>  $datype[$daid][$daoffer]['type']);
												$docda1 =array("da_usage" => $type_event[$evt], "da_cnt" => intval($valueda['cnt']), "da_qty" => $valueda['qty'], "da_vol" => intval($valueda['vol']));
												if(!empty($docda1)){ 
													$doca[] = array_merge($doc_da,$docda1); 
												} 
											}
                                    }                                    
                                }    
                                }
                            }
                            if (!empty($doca)) {
                                $doc1 = array("da"=>$doca);
                            }
                            if (!empty($usage)) {
                              $doc = array("site_name"=>$location[$site_id]['name'], "site_code"=>$site_id);	
                              $document2 = array_merge($doc, $usage, $doc1);
                              $document3[] = $document2;	
                            }
                        } // site
                        if ((!empty($document3)) || (!empty($usage_inc))) {
                            $doc = array("usage_op" => $document3);
                            if(empty($document3)) $doc=array();
                            if(!empty($usage_inc)) {
                                  $document2 = array_merge( $doc_operateur, $usage_inc, $doc);
                            }else {
                                  $document2 = array_merge( $doc_operateur, $doc);
                            }
                            $document4[] = $document2;
                        }
                    }
                } // operateur
                if (!empty($document4)) {
                    $doc = array("usage" => $document4);
                    $document = array_merge($document, $doc);
                }
                // traitement du roaming $cbm_roaming1[$msisdn][$mcc][$subclass]['voice_o_amnt']
                $document1 = array();
                foreach ($cbm_roaming1[$msisdn] as $mcc =>&$value2) { // mcc
                    foreach ($value2 as $op_id =>&$value1) { // op_id
                        $usage = array(); // on affiche que les valeurs non nulles
                        $doc1 = array("roaming_mcc" => substr($mcc,0,3), "roaming_country" => $mcc_country[$mcc]['country'],"network_code" => substr($mcc,3,2), "network_name" => $mcc_country[$mcc]['network'], "country_name" =>  $country[$op_id], "country_code" => $op_id);
                        // ougoing
                        if ($value1['voice_o_cnt'] >0) $usage['voice_o_cnt'] = $value1['voice_o_cnt'];
                        if ($value1['voice_o_main_vol'] >0) $usage['voice_o_main_vol'] = $value1['voice_o_main_vol'];
                        if ($value1['voice_o_amnt'] >0) $usage['voice_o_amnt'] = $value1['voice_o_amnt'];
                        if ($value1['voice_o_amnt1'] >0) $usage['voice_o_amnt1'] = $value1['voice_o_amnt1'];
                        if ($value1['voice_o_bndl_vol'] >0) $usage['voice_o_bndl_vol'] = $value1['voice_o_bndl_vol'];
                        if ($value1['sms_o_main_cnt'] >0) $usage['sms_o_main_cnt'] = $value1['sms_o_main_cnt'];
                        if ($value1['sms_o_bndl_cnt'] >0) $usage['sms_o_bndl_cnt'] = $value1['sms_o_bndl_cnt'];
                        if ($value1['sms_o_amnt'] >0) $usage['sms_o_amnt'] = $value1['sms_o_amnt'];
                        if ($value1['sms_o_amnt1'] >0) $usage['sms_o_amnt1'] = $value1['sms_o_amnt1'];
                        if ($value1['data_o_main_vol'] >0) $usage['data_o_main_vol'] = $value1['data_o_main_vol'];
                        if ($value1['data_amnt'] >0) $usage['data_amnt'] = $value1['data_amnt'];
                        if ($value1['data_amnt1'] >0) $usage['data_amnt1'] = $value1['data_amnt1'];
                        if ($value1['data_o_bndl_vol'] >0) $usage['data_o_bndl_vol'] = $value1['data_o_bndl_vol'];
                        $duration_out += $value1['voice_o_main_vol'] + $value1['voice_o_bndl_vol'];
                        $sms_out += $value1['sms_o_main_cnt'] + $value1['sms_o_bndl_cnt'];
                        $voice_out +=  $value1['voice_o_cnt'] ;
                        $revenue_v_out += $value1['voice_o_amnt'];
                        $revenue_v_out1 += $value1['voice_o_amnt1'];
                        $revenue_s_out += $value1['sms_o_amnt'];
                        $revenue_s_out1 += $value1['sms_o_amnt1'];
                        $revenue_d_out += $value1['data_amnt'];
                        $revenue_d_out1 += $value1['data_amnt1'];
                        $revenue_tot += $value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt'];
                        $revenue_tot1 += $value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1'];
                        $revenue_out += $value1['voice_o_amnt'] + $value1['sms_o_amnt']+ $value1['data_amnt'] ;
                        $revenue_out1 += $value1['voice_o_amnt1'] + $value1['sms_o_amnt1']+ $value1['data_amnt1'] ;
                        $total_data += $value1['data_o_bndl_vol'] + $value1['data_o_main_vol'];                                                
                        // traitement des da
                        $doca = array();$doc2 = array();
                        foreach($cbmr_da1[$msisdn][$mcc][$op_id] as $daid => &$valueda1) {
                            if($daid>0) {
                                foreach($valueda1 as $evt => &$valueda2) { 
										foreach($valueda2 as $daoffer => &$valueda3) {
											foreach($valueda3 as $unit => &$valueda) {
												$doc_da = array("da_id" => $daid, "da_name" =>  $datype[$daid][$daoffer]['name'], "da_offer" => $daoffer, "da_category" =>  $datype[$daid][$daoffer]['category'], "da_unit"=>$daunit[$unit], "da_type" =>  $datype[$daid][$daoffer]['type']);
												$docda1 =array("da_usage" => $type_event[$evt], "da_cnt" => intval($valueda['cnt']), "da_qty" => $valueda['qty'], "da_vol" => intval($valueda['vol']));
												if(!empty($docda1)){ 
													$doca[] = array_merge($doc_da,$docda1); 
												} 
											}
                                    }                                    
                                }    
                            }
                        }
                        if (!empty($doca)) {
                            $doc2 = array("da"=>$doca);
                        }
                        if(!empty($usage)) {
                            $document2 = array_merge($doc1, $usage, $doc2);
                            $document1[] = $document2;	
                        }
                    }
                }
                if (!empty($document1)) {
                    $doc = array("roaming" => $document1);
                    $document = array_merge($document, $doc);
                }				  
                // insertion
                $total=array();
                if($duration_out >0) $total['voice_o_tot_vol'] = $duration_out;
                if($voice_out >0) $total['voice_o_tot_cnt'] = $voice_out;
                if($duration_in >0) $total['voice_i_tot_vol'] = $duration_in;
                if($voice_in >0) $total['voice_i_tot_cnt'] = $voice_in;
                if($sms_in >0)$total['sms_i_tot_vol'] = $sms_in;
                if($sms_in >0)$total['sms_i_tot_cnt'] = $sms_in;
                if($revenue_in >0)$total['revenue_i_tot_amnt'] = $revenue_in;
                if($sms_out >0)$total['sms_o_tot_vol'] = $sms_out;
                if($sms_out >0)$total['sms_o_tot_cnt'] = $sms_out;
                if($revenue_v_out >0)$total['rev_v_o_main_amnt'] = $revenue_v_out;
                if($revenue_s_out >0)$total['rev_s_o_main_amnt'] = $revenue_s_out;
                if($revenue_out >0)$total['rev_o_tot_amnt'] = $revenue_out;
                if($revenue_v_out1 >0)$total['rev_v_o_main_amnt1'] = $revenue_v_out1;
                if($revenue_s_out1 >0)$total['rev_s_o_main_amnt1'] = $revenue_s_out1;
                if($revenue_out1 >0)$total['rev_o_tot_amnt1'] = $revenue_out1;
                if($revenue_tot >0)$total['rev_tot_amnt'] = $revenue_tot;
                if($revenue_tot1 >0)$total['rev_tot_amnt1'] = $revenue_tot1;
                // lanycredit
                if($revenue_v_ln >0) $total['rev_v_ln'] = $revenue_v_ln;
                if($revenue_s_ln >0) $total['rev_s_ln'] = $revenue_s_ln;                             
 
                if(!empty($total)) {
                    $doc = array("total" => $total);
                    $document = array_merge($document, $doc);                      
                    //$collection_inter->insert($document);
                }

                if($usage_iter < 10000)  {
                    $bulk->insert($document);
                    $usage_iter++;
                }
                else {
                    $bulk->insert($document);
                    $mng = $manager->executeBulkWrite('cbm.daily_usage_international', $bulk, $writeConcern);
                    if ( ( $fact % 100) == 0 ) echo "Inseted ".($fact*10000)." usage International docs ". (time() - $t)."\n";
                    $bulk = new MongoDB\Driver\BulkWrite();
                    $usage_iter = 0;
                    $fact++;
                }
            }
            if ($usage_iter != 0 ) {
                echo "Inserting remaining usage international docs : ".$usage_iter."\n";
                $manager->executeBulkWrite('cbm.daily_usage_international', $bulk, $writeConcern);
            }
			
            echo "duree du jour : " . (time()-$t) . "\n";	
        } // end day
    } // end month
} // end year
exec('kill -9 ' . getmypid());
?>

