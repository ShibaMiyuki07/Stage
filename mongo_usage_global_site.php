<?php
error_reporting(E_ERROR);

include ("utils/date.php");
include ("utils/utils_db.php");
include ("utils/functions.php");
include ('utils/utils_routines.php');

$debut = time();

if($argv[1] != null){
	$d1 = $argv[1];
	$d2 = $argv[1];
}

if($argv[1] != null && $argv[2] != null){
	$d2 = $argv[2];
}

if(isset($argv[3])) $tranche = $argv[3];

echo "Database handset\n";

//Get all sites and their secteur and region
tep_db_connect();
$sites = array();
$query = tep_db_query("SELECT sig_code_site site, sig_nom_site nom_site, max(sig_secteur_name_v3) secteur 
FROM DM_RF.rf_sig_cell_krill_v3 sig
group by sig_code_site,sig_nom_site;");
while($value = tep_db_fetch_array($query)){
	 $sites[$value['site']]['secteur'] = $value['secteur'];
	 $sites[$value['site']]['nom_site'] = $value['nom_site'];  	 
}

echo "Get all sites\n";

echo "traitement du " . $d1 . " au " . $d2 . "\n";
$offset = 3*1000*3600;

while($d1<=$d2){
    
    $newDate = date('Y-m-d', strtotime($d1. ' -1 months'));
    $prevDate = substr($newDate,0,4).substr($newDate,5,2);
    $date  = substr($d1,0,4).substr($d1,5,2);


    //Get all parti_id and their segments
    $segments = array();
    $filter  = array('day' => $prevDate);
    $options = array();
    $query = new \MongoDB\Driver\Query($filter, $options);
    $cursor = $cbm_manager->executeQuery('cbm.segment', $query);
    $it = new \IteratorIterator($cursor);
    $it->rewind(); // Very important

    while($document = $it->current()) {
            $json = json_encode($document);
            $value = json_decode($json,true);
            $segments[$value['party_id']] = $value['vbs_Segment_month'];
            $it->next();
    }

    echo "Get all segments\n";
    // on contruit les référentiel depuis les usages
    $bundle = array();
    $recharge = array();
    $mcc= array();
    $start = new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000);
    $end =   new MongoDB\BSON\UTCDateTime(strtotime($d1 ." 23:59:59")*1000);

    $filter  = array('day' => array('$gte' => $start, '$lte' => $end));
    //$filter  = array('day' => array('$gte' => $start, '$lte' => $end));
    //$sort = array('billing_type' => 1,'market' => 1,'pp_name' => 1);
    //$options =  array('sort' => $sort, 'allowDiskUse' => 1, 'batchSize' => 1000, 'timeout' =>1);
    $options =  array( 'allowDiskUse' => 1, 'batchSize' => 1000, 'noCursorTimeout' => true);
    $query = new \MongoDB\Driver\Query($filter, $options);
    $cursor = $cbm_manager->executeQuery('cbm.daily_usage', $query);

    //create collection INTER 
    try {
        $cbm_manager->executeCommand('test', new \MongoDB\Driver\Command(["create" => "daily_usage_TMP"]));
    }
    catch (MongoDB\Driver\Exception\Exception $e) {
        var_dump($e);
    }

    //create index

    $command = new MongoDB\Driver\Command([
        "createIndexes" => "daily_usage_TMP",
        "indexes"       => [[
        "name" => "DI",
        "key"  => [ "DI" => 1],
        "ns"   => "test.daily_usage_TMP",
    ]],
    ]);

    $mng = $cbm_manager->executeCommand("test", $command);


    //$zero = true;
    $it = new \IteratorIterator($cursor);
    $it->rewind(); // Very important
    $bulk = new MongoDB\Driver\BulkWrite();
    $count=0;
    $push=0;
    $doc=array();
    $lenth=0;
    echo "Get all daily_usages\n";
    $counter=0;
    while($document = $it->current()) 
    {
    
        if($counter>=10000){ 
            $mng = $cbm_manager->executeBulkWrite('test.daily_usage_TMP', $bulk, $writeConcern);
            $bulk = new MongoDB\Driver\BulkWrite();
            $counter=0;
        }

        $json = json_encode($document);
        $value = json_decode($json,true);

        $day=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000);
        $billing_type = $value['billing_type'];
        $market = $value['market'];
        $pp_name = $value['pp_name'];
        
        $seg= $segments[$value['party_id']];
        $site=$value['loc_code'];

        if(array_key_exists("usage",$value)){
            foreach($value['usage'] as $label => $value1) 
            {
                if(!array_key_exists("usage_op",$value1))
                {
                    $op_code = $value1['op_code'];
                    $usage_type="usage";
                    $DI=md5($billing_type.$market.$pp_name.$op_code.$seg.$site.$usage_type);
                    /*$x=array_filter($doc, function($d)  use ($seg,$usage_type,$day,$billing_type,$market,$pp_name,$op_code,$secteur) {
                        return ($d['segment']==$seg && $d['usage_type']==$usage_type && $d['day']==$day && $d['billing_type'] == $billing_type && $d['market'] == $market && $d['pp_name'] == $pp_name && $d['op_code'] == $op_code && $d['secteur'] == $secteur );
                    }) ?: false;*/

                    $x=array();
                    $x['day']=$day;
                    $x['billing_type']= $billing_type;
                    $x['market']= $market;
                    $x['pp_name']= $pp_name;
                    $x['op_code']= $op_code;
                    $x['segment']= $seg;
                    $x['site_code']= $site;
                    $x['usage_type']= $usage_type;
                    $x['DI']=$DI;
                    if(array_key_exists("sms_i_cnt",$value1)) $x['sms_i_cnt'] = $value1['sms_i_cnt'];
                    else $x['sms_i_cnt'] = 0;
                    if(array_key_exists("voice_i_cnt",$value1)) $x['voice_i_cnt'] = $value1['voice_i_cnt'];
                    else $x['voice_i_cnt'] = 0;
                    if(array_key_exists("voice_i_vol",$value1)) $x['voice_i_vol'] = $value1['voice_i_vol'];
                    else $x['voice_i_vol'] = 0;
                    if(array_key_exists("voice_i_amnt",$value1)) $x['voice_i_amnt'] = $value1['voice_i_amnt'];
                    else $x['voice_i_amnt'] = 0;
                    $x['voice_o_cnt'] = 0;
                    $x['voice_o_main_vol'] = 0;
                    $x['voice_o_amnt']= 0;
                    $x['voice_o_bndl_vol'] = 0;
                    $x['sms_o_main_cnt'] = 0;
                    $x['sms_o_bndl_cnt'] = 0;
                    $x['sms_o_amnt'] = 0;		
                    $x['data_main_vol'] = 0;
                    $x['data_amnt'] = 0;
                    $x['usage_2G'] = 0;
                    $x['usage_3G'] = 0;
                    $x['usage_4G_TDD'] = 0;
                    $x['usage_4G_FDD'] = 0;
                    $x['data_bndl_vol'] = 0;
                    $x['voice_vas_cnt'] = 0;
                    $x['voice_vas_amnt'] = 0;
                    $x['voice_vas_main_vol'] = 0;
                    $x['voice_vas_bndl_vol'] = 0;		
                    $x['sms_vas_cnt'] = 0;
                    $x['sms_vas_bndl_cnt'] = 0;			
                    $x['sms_vas_amnt'] = 0;	

                    $bulk->insert($x);

                }
                else
                {
                    foreach($value1['usage_op'] as $i => $value2)
                    {
                        $op_code = $value1['op_code'];
                        $seg= $segments[$value['party_id']];
                        $site=$value2['site_code'];
                        $usage_type="usage";
                        $DI=md5($billing_type.$market.$pp_name.$op_code.$seg.$site.$usage_type);
                        
                        $x=array();
                        $x['day']=$day;
                        $x['billing_type']= $billing_type;
                        $x['market']= $market;
                        $x['pp_name']= $pp_name;
                        $x['op_code']= $op_code;
                        $x['segment']= $seg;
                        $x['site_code']= $site;
                        $x['usage_type']=$usage_type;
                        $x['DI']=$DI;
                        if($value1['sms_i_cnt']>=0 && $value1['sms_i_cnt']!=NULL) {
                            $x['sms_i_cnt'] = $value1['sms_i_cnt'];
                        }
                        else {$x['sms_i_cnt'] = 0;
                        } 
                        if($value1['voice_i_cnt']>=0 && $value1['sms_i_cnt']!=NULL) $x['voice_i_cnt'] = $value1['voice_i_cnt'];
                        else $x['voice_i_cnt'] = 0;
                        if($value1['voice_i_vol']>=0 && $value1['voice_i_vol']!=NULL) $x['voice_i_vol'] = $value1['voice_i_vol'];
                        else $x['voice_i_vol'] = 0;
                        if($value1['voice_i_amnt']>=0 && $value1['voice_i_amnt']!=NULL) $x['voice_i_amnt'] = $value1['voice_i_amnt'];
                        else $x['voice_i_amnt'] = 0;
                        if($value2['voice_o_cnt']>=0 && $value2['voice_o_cnt']!=NULL) $x['voice_o_cnt'] = $value2['voice_o_cnt'];
                        else $x['voice_o_cnt']=0;
                        if($value2['voice_o_main_vol']>=0 && $value2['voice_o_main_vol']!=NULL) $x['voice_o_main_vol'] = $value2['voice_o_main_vol'];
                        else $x['voice_o_main_vol']=0;
                        if($value2['voice_o_amnt']>=0 && $value2['voice_o_amnt']!=NULL) $x['voice_o_amnt']= $value2['voice_o_amnt'];
                        else $x['voice_o_amnt']=0;
                        if($value2['voice_o_bndl_vol']>=0 && $value2['voice_o_bndl_vol']!=NULL) $x['voice_o_bndl_vol'] = $value2['voice_o_bndl_vol'];
                        else $x['voice_o_bndl_vol'] =0;
                        if($value2['sms_o_main_cnt']>=0 && $value2['sms_o_main_cnt']!=NULL) $x['sms_o_main_cnt'] = $value2['sms_o_main_cnt'];
                        else $x['sms_o_main_cnt'] =0;
                        if($value2['sms_o_bndl_cnt']>=0 && $value2['sms_o_bndl_cnt']!=NULL) $x['sms_o_bndl_cnt'] = $value2['sms_o_bndl_cnt'];
                        else $x['sms_o_bndl_cnt']=0;
                        if($value2['sms_o_amnt']>=0 && $value2['sms_o_amnt']!=NULL) $x['sms_o_amnt'] = $value2['sms_o_amnt'];
                        else $x['sms_o_amnt'] = 0;
                        if($value2['data_main_vol']>=0 && $value2['data_main_vol']!=NULL) $x['data_main_vol'] = $value2['data_main_vol'];
                        else $x['data_main_vol'] = 0;
                        if($value2['data_amnt']>=0 && $value2['data_amnt']!=NULL) $x['data_amnt'] = $value2['data_amnt'];
                        else $x['data_amnt'] = 0;
                        if($value2['usage_2G']>=0 && $value2['usage_2G']!=NULL) $x['usage_2G'] = $value2['usage_2G'];
                        else $x['usage_2G'] = 0;
                        if($value2['usage_3G']>=0 && $value2['usage_3G']!=NULL) $x['usage_3G'] = $value2['usage_3G'];
                        else $x['usage_3G'] = 0;
                        if($value2['usage_4G_TDD']>=0 && $value2['usage_4G_TDD']!=NULL) $x['usage_4G_TDD'] = $value2['usage_4G_TDD'];
                        else $x['usage_4G_TDD'] = 0;
                        if($value2['usage_4G_FDD']>=0 && $value2['usage_4G_FDD']!=NULL) $x['usage_4G_FDD'] = $value2['usage_4G_FDD'];
                        else $x['usage_4G_FDD'] = 0;
                        if($value2['data_bndl_vol']>=0 && $value2['data_bndl_vol']!=NULL) $x['data_bndl_vol'] = $value2['data_bndl_vol'];
                        else $x['data_bndl_vol'] = 0;
                        if($value2['voice_vas_cnt']>=0 && $value2['voice_vas_cnt']!=NULL) $x['voice_vas_cnt'] = $value2['voice_vas_cnt'];
                        else $x['voice_vas_cnt'] = 0;
                        if($value2['voice_vas_amnt']>=0 && $value2['voice_vas_amnt']!=NULL) $x['voice_vas_amnt'] = $value2['voice_vas_amnt'];
                        else $x['voice_vas_amnt'] = 0;
                        if($value2['voice_vas_main_vol']>=0 && $value2['voice_vas_main_vol']!=NULL) $x['voice_vas_main_vol'] = $value2['voice_vas_main_vol'];
                        else $x['voice_vas_main_vol'] = 0;
                        if($value2['voice_vas_bndl_vol']>=0 && $value2['voice_vas_bndl_vol']!=NULL) $x['voice_vas_bndl_vol'] = $value2['voice_vas_bndl_vol'];
                        else $x['voice_vas_bndl_vol'] = 0;
                        if($value2['sms_vas_cnt']>=0 && $value2['sms_vas_cnt']!=NULL) $x['sms_vas_cnt'] = $value2['sms_vas_cnt'];
                        else $x['sms_vas_cnt'] = 0;
                        if($value2['sms_vas_bndl_cnt']>=0 && $value2['sms_vas_bndl_cnt']!=NULL) $x['sms_vas_bndl_cnt'] = $value2['sms_vas_bndl_cnt'];
                        else $x['sms_vas_bndl_cnt'] = 0;
                        if($value2['sms_vas_amnt']>=0 && $value2['sms_vas_amnt']!=NULL) $x['sms_vas_amnt'] = $value2['sms_vas_amnt'];
                        else $x['sms_vas_amnt'] = 0;
                        
                        $bulk->insert($x);
                    }
                
                }	
            }

        
        }
        if(array_key_exists("topup",$value)){
            foreach($value['topup'] as $label => $value1) {
                foreach($value1['recharge'] as $i => $value2) {
                    $rec_type=$value1['rec_type'];
                    $rec_code=$value1['rec_code'];
                    $seg= $segments[$value['party_id']];
                    $site=$value2['site_code'];
                    $usage_type="topup";
                    $DI=md5($billing_type.$market.$pp_name.$rec_type.$rec_code.$seg.$site.$usage_type);
                    $x=array();
                    $x['day']=$day;
                    $x['billing_type']= $billing_type;
                    $x['market']= $market;
                    $x['pp_name']= $pp_name;
                    $x['rec_type']= $rec_type;
                    $x['rec_code']= $rec_code;
                    $x['segment']=$seg;
                    $x['site_code']= $site;
                    $x['usage_type']=$usage_type;
                    $x['DI']=$DI;
                    if($value2['rec_cnt']>=0 && $value2['rec_cnt']!=NULL) {
                        $x['rec_cnt'] = $value2['rec_cnt'];
                    }
                    else {$x['rec_cnt'] = 0;
                    } 
                    if($value2['rec_amnt']>=0 && $value2['rec_amnt']!=NULL) $x['rec_amnt'] = $value2['rec_amnt'];
                    else $x['rec_amnt'] = 0;
                    $bulk->insert($x);
                }	
                
            }
        }
        if(array_key_exists("bundle",$value)){
            foreach($value['bundle'] as $label => $value1) {
                foreach($value1['subscription'] as $i => $value2) {
                    $bndle_name=$value1['bndle_name'];
                    $bndle_code=$value1['bndle_code'];
					$bndle_group=$value1['bndle_group'];
                    $seg= $segments[$value['party_id']];
                    $site=$value2['site_code'];
                    $usage_type="bundle";
                    $DI=md5($billing_type.$market.$pp_name.$bndle_name.$bndle_code.$seg.$site.$usage_type);
                    
                    $x=array();
                    $x['day']=$day;
                    $x['billing_type']= $billing_type;
                    $x['market']= $market;
                    $x['pp_name']= $pp_name;
                    $x['bndle_name']= $bndle_name;
                    $x['bndle_code']= $bndle_code;
					$x['bndle_group']= $bndle_group;
                    $x['segment']=$seg;
                    $x['site_code']= $site;
                    $x['usage_type']=$usage_type;
                    $x['DI']=$DI;
                    if($value2['bndle_cnt']>=0 && $value2['bndle_cnt']!=NULL) {
                        $x['bndle_cnt'] = $value2['bndle_cnt'];
                    }
                    else {$x['bndle_cnt'] = 0;
                    } 
                    if($value2['bndle_amnt']>=0 && $value2['bndle_amnt']!=NULL) $x['bndle_amnt'] = $value2['bndle_amnt'];
                    else $x['bndle_amnt'] = 0;
                    $bulk->insert($x);
                }
            }
        }
        if(array_key_exists("om",$value)){
            foreach($value['om'] as $label => $value1) {
                foreach($value1['transaction'] as $i => $value2) {
                    $tr_type=$value1['transaction_type'];
                    $seg= $segments[$value['party_id']];
                    $site=$value2['site_code'];
                    $usage_type="om";
                    $DI=md5($billing_type.$market.$pp_name.$tr_type.$seg.$site.$usage_type);
                    $x=array();
                    $x['day']=$day;
                    $x['billing_type']= $billing_type;
                    $x['market']= $market;
                    $x['pp_name']= $pp_name;
                    $x['transaction_type']= $tr_type;
                    $x['segment']=$seg;
                    $x['site_code']= $site;
                    $x['usage_type']=$usage_type;
                    $x['DI']=$DI;
                    if($value2['om_cnt']>=0 && $value2['om_cnt']!=NULL) {
                        $x['om_cnt'] = $value2['om_cnt'];
                    }
                    else {$x['om_cnt'] = 0;} 
                    if($value2['om_amnt']>=0 && $value2['om_amnt']!=NULL) $x['om_amnt'] = $value2['om_amnt'];
                    else $x['om_amnt'] = 0;
                    if($value2['om_tr_amnt']>=0 && $value2['om_tr_amnt']!=NULL) $x['om_tr_amnt'] = $value2['om_tr_amnt'];
                    else $x['om_tr_amnt'] = 0;
                    if($value2['om_r_cnt']>=0 && $value2['om_r_cnt']!=NULL) $x['om_r_cnt'] = $value2['om_r_cnt'];
                    else $x['om_r_cnt'] = 0;
                    if($value2['om_r_tr_amnt']>=0 && $value2['om_r_tr_amnt']!=NULL) $x['om_r_tr_amnt'] = $value2['om_r_tr_amnt'];
                    else $x['om_r_tr_amnt']=0;
                    $bulk->insert($x);
                }
            }
        }
        if(array_key_exists("EC",$value)){
            foreach($value['EC'] as $label => $value1) {
                $seg= $segments[$value['party_id']];
                $site=$value1['site_code'];
                $usage_type="ec";
                $DI=md5($billing_type.$market.$pp_name.$seg.$site.$usage_type);
                
                    $x=array();
                    $x['day']=$day;
                    $x['billing_type']= $billing_type;
                    $x['market']= $market;
                    $x['pp_name']= $pp_name;
                    $x['segment']=$seg;
                    $x['site_code']= $site;
                    $x['usage_type']=$usage_type;
                    $x['DI']=$DI;
                    if($value1['ec_loan']>=0 && $value1['ec_loan']!=NULL) {
                        $x['ec_loan'] = $value1['ec_loan'];
                    }
                    else {$x['ec_loan'] = 0;} 
                    if($value1['ec_qty']>=0 && $value1['ec_qty']!=NULL) $x['ec_qty'] = $value1['ec_qty'];
                    else $x['ec_qty'] = 0;
                    if($value1['ec_fees']>=0 && $value1['ec_fees']!=NULL) $x['ec_fees'] = $value1['ec_fees'];
                    else $x['ec_fees'] = 0;
                    if($value1['ec_payback']>=0 && $value1['ec_payback']!=NULL) $x['ec_payback'] = $value1['ec_payback'];
                    else $x['ec_payback'] = 0;
                    if($value1['ca_reactivation']>=0 && $value1['ca_reactivation']!=NULL) $x['ca_reactivation'] = $value1['ca_reactivation'];
                    else $x['ca_reactivation']=0;
                    $bulk->insert($x);
                
            }
        }
        if(array_key_exists("roaming",$value)){
            foreach($value['roaming'] as $label => $value1) {
                $op_code = $value1['op_code'];
                $roaming_mcc = $value1['roaming_mcc'];
                $roaming_country = $value1['roaming_country'];
                $network_code = $value1['network_code'];
                $network_name = $value1['network_name'];
                $seg= $segments[$value['party_id']];
                $site=$value['loc_code'];
                $usage_type="roaming";
                $DI=md5($billing_type.$market.$pp_name.$op_code.$roaming_mcc.$roaming_country.$network_code.$network_name.$seg.$site.$usage_type);
                $x=array();
                $x['day']=$day;
                $x['billing_type']= $billing_type;
                $x['market']= $market;
                $x['pp_name']= $pp_name;
                $x['op_code']= $op_code;
                $x['roaming_mcc'] = $roaming_mcc ;
                $x['roaming_country'] = $roaming_country ;
                $x['network_code'] = $network_code ;
                $x['network_name'] = $network_name ;
                $x['segment']=$seg;
                $x['site_code']= $site;
                $x['usage_type']= $usage_type;
                $x['DI']=$DI;
                if($value1['voice_o_cnt']>=0 && $value1['voice_o_cnt']!=NULL) $x['voice_o_cnt'] = $value1['voice_o_cnt'];
                else $x['voice_o_cnt'] = 0;
                if($value1['voice_o_bndl_vol']>=0 && $value1['voice_o_bndl_vol']!=NULL) $x['voice_o_bndl_vol'] = $value1['voice_o_bndl_vol'];
                else $x['voice_o_bndl_vol'] = 0;
                if($value1['voice_o_main_vol']>=0 && $value1['voice_o_main_vol']!=NULL) $x['voice_o_main_vol'] = $value1['voice_o_main_vol'];
                else $x['voice_o_main_vol'] = 0;
                if($value1['voice_o_amnt']>=0 && $value1['voice_o_amnt']!=NULL) $x['voice_o_amnt'] = $value1['voice_o_amnt'];
                else $x['voice_o_amnt']=0;
                if($value1['sms_o_main_cnt']>=0 && $value1['sms_o_main_cnt']!=NULL) $x['sms_o_main_cnt'] = $value1['sms_o_main_cnt'];
                else $x['sms_o_main_cnt'] = 0;
                if($value1['sms_o_bndl_cnt']>=0 && $value1['sms_o_bndl_cnt']!=NULL) $x['sms_o_bndl_cnt'] = $value1['sms_o_bndl_cnt'];
                else $x['sms_o_bndl_cnt'] = 0;
                if($value1['data_main_vol']>=0 && $value1['data_main_vol']!=NULL) $x['data_main_vol'] = $value1['data_main_vol'];
                else $x['data_main_vol'] = 0;
                if($value1['data_bndl_vol']>=0 && $value1['data_bndl_vol']!=NULL) $x['data_bndl_vol'] = $value1['data_bndl_vol'];
                else $x['data_bndl_vol']=0;
                if($value1['sms_o_amnt']>=0 && $value1['sms_o_amnt']!=NULL) $x['sms_o_amnt'] = $value1['sms_o_amnt'];
                else $x['sms_o_amnt'] = 0;
                if($value1['data_amnt']>=0 && $value1['data_amnt']!=NULL) $x['data_amnt'] = $value1['data_amnt'];
                else $x['data_amnt']=0;
                $bulk->insert($x);
                    
            }
        }
        $lenth++;
        if($lenth%50000==0)
            echo $lenth . "docs sont traités\n";
        $counter++;
        $it->next();
    }
    // Insert the rest
    try{
    if($counter != 0 && $bulk != null) $mng = $cbm_manager->executeBulkWrite('test.daily_usage_TMP', $bulk, $writeConcern);
    }
    catch (MongoDB\Driver\Exception\Exception $e) {
        var_dump($e);
    }




    //$liste_indexes = [["name" => "party_id","key"  => [ "party_id" => 1]],["name" => "subs_id","key"  => [ "subs_id" => 1]]];
    //$command = new MongoDB\Driver\Command(["createIndexes" => "daily_contracts", "indexes" =>$liste_indexes]);

    $filter=array();
    //$filter  = array('day' => array('$gte' => $start, '$lte' => $end),'usage'=> array('$exists'=> 1));
    //$filter  = array('day' => array('$gte' => $start, '$lte' => $end));
    $sort = array('DI' => 1);
    //$options =  array('sort' => $sort, 'allowDiskUse' => 1, 'batchSize' => 1000, 'timeout' =>1);
    $options =  array( 'sort'=>$sort, 'allowDiskUse' => 1, 'batchSize' => 1000, 'noCursorTimeout' => true);
    $query = new \MongoDB\Driver\Query($filter, $options);
    $cursor = $cbm_manager->executeQuery('test.daily_usage_TMP', $query);

    //$zero = true;
    $it = new \IteratorIterator($cursor);
    $it->rewind(); // Very important
    $bulk = new MongoDB\Driver\BulkWrite();
    echo "Get all daily_usages_US\n";
    
    $di=" ";
    $counter=0;
    while($document = $it->current()) 
    {
        $json = json_encode($document);
        $value = json_decode($json,true);
        //var_dump($value);
        if($value['DI']==$di){
            if($value['usage_type']=='usage'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['op_code']= $value['op_code'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
    //          $x['DI']=$value['DI'];
                $x['sms_i_cnt'] += $value['sms_i_cnt'];
                $x['voice_i_cnt'] += $value['voice_i_cnt'];
                $x['voice_i_vol'] += $value['voice_i_vol'];
                $x['voice_i_amnt'] += $value['voice_i_amnt'];
                $x['voice_o_cnt'] += $value['voice_o_cnt'];
                $x['voice_o_main_vol'] += $value['voice_o_main_vol'];
                $x['voice_o_amnt']+= $value['voice_o_amnt'];
                $x['voice_o_bndl_vol'] += $value['voice_o_bndl_vol'];
                $x['sms_o_main_cnt'] += $value['sms_o_main_cnt'];
                $x['sms_o_bndl_cnt'] += $value['sms_o_bndl_cnt'];
                $x['sms_o_amnt'] += $value['sms_o_amnt'];		
                $x['data_main_vol'] += $value['data_main_vol'];
                $x['data_amnt'] += $value['data_amnt'];
                $x['usage_2G'] += $value['usage_2G'];
                $x['usage_3G'] += $value['usage_3G'];
                $x['usage_4G_TDD'] += $value['usage_4G_TDD'];
                $x['usage_4G_FDD'] += $value['usage_4G_FDD'];
                $x['data_bndl_vol'] += $value['data_bndl_vol'];
                $x['voice_vas_cnt'] += $value['voice_vas_cnt'];
                $x['voice_vas_amnt'] += $value['voice_vas_amnt'];
                $x['voice_vas_main_vol'] += $value['voice_vas_main_vol'];
                $x['voice_vas_bndl_vol'] += $value['voice_vas_bndl_vol'];		
                $x['sms_vas_cnt'] += $value['sms_vas_cnt'];
                $x['sms_vas_bndl_cnt'] += $value['sms_vas_bndl_cnt'];			
                $x['sms_vas_amnt'] += $value['sms_vas_amnt'];
            }
            if($value['usage_type']=='roaming'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['op_code']= $value['op_code'];
                $x['roaming_mcc'] = $value['roaming_mcc'] ;
                $x['roaming_country'] = $value['roaming_country'] ;
                $x['network_code'] = $value['network_code'] ;
                $x['network_name'] = $value['network_name'] ;
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
    //            $x['DI']=$value['DI'];
                $x['voice_o_cnt'] += $value['voice_o_cnt'];
                $x['voice_o_bndl_vol'] += $value['voice_o_bndl_vol'];
                $x['voice_o_main_vol'] += $value['voice_o_main_vol'];
                $x['voice_o_amnt'] += $value['voice_o_amnt'];
                $x['sms_o_main_cnt'] += $value['sms_o_main_cnt'];
                $x['sms_o_bndl_cnt'] += $value['sms_o_bndl_cnt'];
                $x['data_main_vol'] += $value['data_main_vol'];
                $x['data_bndl_vol'] += $value['data_bndl_vol'];
                $x['sms_o_amnt'] += $value['sms_o_amnt'];
                $x['data_amnt'] += $value['data_amnt'];
            }
            if($value['usage_type']=='ec'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
                //$x['DI']=$value['DI'];
                $x['ec_loan'] += $value['ec_loan'];
                $x['ec_qty'] += $value['ec_qty'];
                $x['ec_fees'] += $value['ec_fees'];
                $x['ec_payback'] += $value['ec_payback'];
                $x['ca_reactivation'] += $value['ca_reactivation'];
            }
            if($value['usage_type']=='om'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['transaction_type']= $value['transaction_type'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
            //  $x['DI']=$value['DI'];
                $x['om_cnt'] += $value['om_cnt'];
                $x['om_amnt'] += $value['om_amnt'];
                $x['om_tr_amnt'] += $value['om_tr_amnt'];
                $x['om_r_cnt'] += $value['om_r_cnt'];
                $x['om_r_tr_amnt'] += $value['om_r_tr_amnt'];
            }
            if($value['usage_type']=='bundle'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['bndle_name']= $value['bndle_name'];
                $x['bndle_code']= $value['bndle_code'];
				$x['bndle_group']= $value['bndle_group'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
            //    $x['DI']=$value['DI'];
                $x['bndle_cnt'] += $value['bndle_cnt'];
                $x['bndle_amnt'] += $value['bndle_amnt'];
            }
            if($value['usage_type']=='topup'){
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['rec_type']= $value['rec_type'];
                $x['rec_code']= $value['rec_code'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
        //      $x['DI']=$value['DI'];
                $x['rec_cnt'] += $value['rec_cnt'];
                $x['rec_amnt'] += $value['rec_amnt'];
            }
        }
        else{
            if($di!=" "){
                $bulk->insert($x);
                $counter++;
            }
            if($value['usage_type']=='usage'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['op_code']= $value['op_code'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
        //        $x['DI']=$value['DI'];
                $x['sms_i_cnt'] = $value['sms_i_cnt'];
                $x['voice_i_cnt'] = $value['voice_i_cnt'];
                $x['voice_i_vol'] = $value['voice_i_vol'];
                $x['voice_i_amnt'] = $value['voice_i_amnt'];
                $x['voice_o_cnt'] = $value['voice_o_cnt'];
                $x['voice_o_main_vol'] = $value['voice_o_main_vol'];
                $x['voice_o_amnt'] = $value['voice_o_amnt'];
                $x['voice_o_bndl_vol'] = $value['voice_o_bndl_vol'];
                $x['sms_o_main_cnt'] = $value['sms_o_main_cnt'];
                $x['sms_o_bndl_cnt'] = $value['sms_o_bndl_cnt'];
                $x['sms_o_amnt'] = $value['sms_o_amnt'];		
                $x['data_main_vol'] = $value['data_main_vol'];
                $x['data_amnt'] = $value['data_amnt'];
                $x['usage_2G'] = $value['usage_2G'];
                $x['usage_3G'] = $value['usage_3G'];
                $x['usage_4G_TDD'] = $value['usage_4G_TDD'];
                $x['usage_4G_FDD'] = $value['usage_4G_FDD'];
                $x['data_bndl_vol'] = $value['data_bndl_vol'];
                $x['voice_vas_cnt'] = $value['voice_vas_cnt'];
                $x['voice_vas_amnt'] = $value['voice_vas_amnt'];
                $x['voice_vas_main_vol'] = $value['voice_vas_main_vol'];
                $x['voice_vas_bndl_vol'] = $value['voice_vas_bndl_vol'];		
                $x['sms_vas_cnt'] = $value['sms_vas_cnt'];
                $x['sms_vas_bndl_cnt'] = $value['sms_vas_bndl_cnt'];		
                $x['sms_vas_amnt'] = $value['sms_vas_amnt'];
            }
            if($value['usage_type']=='roaming'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['op_code']= $value['op_code'];
                $x['roaming_mcc'] = $value['roaming_mcc'] ;
                $x['roaming_country'] = $value['roaming_country'] ;
                $x['network_code'] = $value['network_code'] ;
                $x['network_name'] = $value['network_name'] ;
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
    //          $x['DI']=$value['DI'];
                $x['voice_o_cnt'] = $value['voice_o_cnt'];
                $x['voice_o_bndl_vol'] = $value['voice_o_bndl_vol'];
                $x['voice_o_main_vol'] = $value['voice_o_main_vol'];
                $x['voice_o_amnt'] = $value['voice_o_amnt'];
                $x['sms_o_main_cnt'] = $value['sms_o_main_cnt'];
                $x['sms_o_bndl_cnt'] = $value['sms_o_bndl_cnt'];
                $x['data_main_vol'] = $value['data_main_vol'];
                $x['data_bndl_vol'] = $value['data_bndl_vol'];
                $x['sms_o_amnt'] = $value['sms_o_amnt'];
                $x['data_amnt'] = $value['data_amnt'];
            }
            if($value['usage_type']=='ec'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['transaction_type']= $value['transaction_type'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
    //            $x['DI']=$value['DI'];
                $x['ec_loan'] = $value['ec_loan'];
                $x['ec_qty'] = $value['ec_qty'];
                $x['ec_fees'] = $value['ec_fees'];
                $x['ec_payback'] = $value['ec_payback'];
                $x['ca_reactivation'] = $value['ca_reactivation'];
            }
            if($value['usage_type']=='om'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['transaction_type']= $value['transaction_type'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
                //$x['DI']=$value['DI'];
                $x['om_cnt'] = $value['om_cnt'];
                $x['om_amnt'] = $value['om_amnt'];
                $x['om_tr_amnt'] = $value['om_tr_amnt'];
                $x['om_r_cnt'] = $value['om_r_cnt'];
                $x['om_r_tr_amnt'] = $value['om_r_tr_amnt'];
            }
            if($value['usage_type']=='bundle'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['bndle_name']= $value['bndle_name'];
                $x['bndle_code']= $value['bndle_code'];
				$x['bndle_group']= $value['bndle_group'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
                //$x['DI']=$value['DI'];
                $x['bndle_cnt'] = $value['bndle_cnt'];
                $x['bndle_amnt'] = $value['bndle_amnt'];
            }
            if($value['usage_type']=='topup'){
                $x=array();
                $x['day']=new MongoDB\BSON\UTCDateTime(strtotime($d1)*1000 + $offset) ;
                $x['billing_type']= $value['billing_type'];
                $x['market']= $value['market'];
                $x['pp_name']= $value['pp_name'];
                $x['rec_type']= $value['rec_type'];
                $x['rec_code']= $value['rec_code'];
                $x['segment']= $value['segment'];
                $x['site_code']= $value['site_code'];
                $x['site_name']=$sites[$value['site_code']]['nom_site'];
                $x['secteur']= $sites[$value['site_code']]['secteur'];
                $x['usage_type']=$value['usage_type'];
                //$x['DI']=$value['DI'];
                $x['rec_cnt'] = $value['rec_cnt'];
                $x['rec_amnt'] = $value['rec_amnt'];
            }
        }

        if($counter>=5000){ 
            //unset($fields['DI']);
            $mng = $cbm_manager->executeBulkWrite('cbm.global_daily_usage', $bulk, $writeConcern);
            echo $counter . "docs sont traités et inserés\n";
            $bulk = new MongoDB\Driver\BulkWrite();
            $counter=0;
            //$counter=0;
        }
        $di=$value['DI'];
        $it->next();
    }
    $bulk->insert($x);

    // Insert the rest
    $mng = $cbm_manager->executeBulkWrite('cbm.global_daily_usage', $bulk, $writeConcern);
    echo "fin du traitement \t" . (time()-$debut) . "\n";
    try{
        $cbm_manager->executeCommand('test', new \MongoDB\Driver\Command(["drop" => "daily_usage_TMP"]));
    }
    catch (MongoDB\Driver\Exception\Exception $e) {
        var_dump($e);
    }

    $d1 = date('Y-m-d', strtotime($d1. ' +1 days'));
}

?>

