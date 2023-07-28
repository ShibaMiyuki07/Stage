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

        if(array_key_exists("usage",$value)){
            foreach($value['usage'] as $label => $value1) 
            {
                if(!array_key_exists("usage_op",$value1))
                {

                    $x=array();
                    $x['day']=$day;
                    if(array_key_exists("sms_i_cnt",$value1)) $x['sms_i_cnt'] = $value1['sms_i_cnt'];
                    else $x['sms_i_cnt'] = 0;
                    if(array_key_exists("voice_i_cnt",$value1)) $x['voice_i_cnt'] = $value1['voice_i_cnt'];
                    else $x['voice_i_cnt'] = 0;
                    if(array_key_exists("voice_i_vol",$value1)) $x['voice_i_vol'] = $value1['voice_i_vol'];
                    else $x['voice_i_vol'] = 0;
                    if(array_key_exists("voice_i_amnt",$value1)) $x['voice_i_amnt'] = $value1['voice_i_amnt'];
                    else $x['voice_i_amnt'] = 0;

                    $bulk->insert($x);

                }
                else
                {
                    foreach($value1['usage_op'] as $i => $value2)
                    {
                        $DI=md5($billing_type.$market.$pp_name.$op_code.$seg.$site.$usage_type);
                        
                        $x=array();
                        $x['day']=$day;
                        if($value1['sms_i_cnt']>=0 && $value1['sms_i_cnt']!=NULL) {
                            $x['sms_i_cnt'] = $value1['sms_i_cnt'];
                        }
                        else {$x['sms_i_cnt'] = 0;
                        } 
                        if($value1['voice_i_cnt']>=0 && $value1['voice_i_cnt']!=NULL) $x['voice_i_cnt'] = $value1['voice_i_cnt'];
                        else $x['voice_i_cnt'] = 0;
                        if($value1['voice_i_vol']>=0 && $value1['voice_i_vol']!=NULL) $x['voice_i_vol'] = $value1['voice_i_vol'];
                        else $x['voice_i_vol'] = 0;
                        if($value1['voice_i_amnt']>=0 && $value1['voice_i_amnt']!=NULL) $x['voice_i_amnt'] = $value1['voice_i_amnt'];
                        else $x['voice_i_amnt'] = 0;
                        $bulk->insert($x);
                    }
                
                }	
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
    //          $x['DI']=$value['DI'];
                $x['sms_i_cnt'] += $value['sms_i_cnt'];
                $x['voice_i_cnt'] += $value['voice_i_cnt'];
                $x['voice_i_vol'] += $value['voice_i_vol'];
                $x['voice_i_amnt'] += $value['voice_i_amnt'];
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
        //        $x['DI']=$value['DI'];
                $x['sms_i_cnt'] = $value['sms_i_cnt'];
                $x['voice_i_cnt'] = $value['voice_i_cnt'];
                $x['voice_i_vol'] = $value['voice_i_vol'];
                $x['voice_i_amnt'] = $value['voice_i_amnt'];
            }
        }

        if($counter>=5000){ 
            //unset($fields['DI']);
            $mng = $cbm_manager->executeBulkWrite('test.global_daily_test', $bulk, $writeConcern);
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
    $mng = $cbm_manager->executeBulkWrite('test.global_daily_test', $bulk, $writeConcern);
    echo "fin du traitement \t" . (time()-$debut) . "\n";

    $d1 = date('Y-m-d', strtotime($d1. ' +1 days'));
}

?>

