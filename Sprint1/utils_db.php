<?php
//include ("/data/script/php/utils/utils_log.php");

$GLOBALS['BI_DB_SERVER']     = "192.168.61.196";
$GLOBALS['BI_DB_USER']       = "krill";
$GLOBALS['BI_DB_PWD']        = "krill123";
//$GLOBALS['LOG'] = new log();

$GLOBALS['BI_DB_SERVER1']     = "192.168.61.202";
$GLOBALS['BI_DB_USER1']       = "root";
$GLOBALS['BI_DB_PWD1']        = "PLATON";

$GLOBALS['MYPOS_DB_SERVER']     = "192.168.61.235";
$GLOBALS['MYPOS_DB_USER']       = "mypos_dash";
$GLOBALS['MYPOS_DB_PWD']        = "mypos_dash@OrnZ";


//BI DB
function tep_db_connect($server='', $username='', $password='', $database='', $link = 'db_link') {
        global $$link;
        $$link = mysqli_connect($GLOBALS['BI_DB_SERVER'] , $GLOBALS['BI_DB_USER'], $GLOBALS['BI_DB_PWD']);
        if ($$link) mysqli_select_db($$link,'DWH');
        return $$link;
}

function tep_db_query($query, $link = 'db_link') {
        global $$link, $logger;
        $result = mysqli_query( $$link,$query) ;//or tep_db_error($query, mysqli_errno($$link), mysqli_error($$link));
        return $result;
}

function tep_db_fetch_array($db_query) {
        return mysqli_fetch_array($db_query, MYSQLI_ASSOC);
}


?>

