<?php

$d1 = date('Y-m-d', strtotime('-1 DAY'));
$d2 = date('Y-m-d', strtotime('-1 DAY'));
function tep_date_raw($date, $reverse = false) {
  if ($reverse) {
    return substr($date, 0, 2) . '-' . substr($date, 3, 2) . '-'. substr($date, 6, 4). ' ' . substr($date,11,8);
  } else {
    return substr($date, 6, 4) . '-' . substr($date, 3, 2) . '-' .substr($date, 0, 2). ' ' . substr($date,11,8);
        //return substr($date, 6, 4) . '-' . substr($date, 0, 2) . '-' .substr($date, 3, 2). ' ' . substr($date,11,8);
  }
}

function tep_date_trunc($date, $reverse = false) {
  if ($reverse) {
    return substr($date, 0, 2) . '-' . substr($date, 3, 2) . '-'. substr($date, 6, 4);
  } else {
    return substr($date, 6, 4) . '-' . substr($date, 3, 2) . '-' .substr($date, 0, 2);
  }
}

function new_date($date) {
    //20190618T11:53:46+0300
    return substr($date, 0, 4) . '-' . substr($date, 4, 2) . '-' .substr($date, 6, 2). ' ' . substr($date,9,8);
}

function new_date_trunc($date) {
    //20190618T11:53:46+0300
    return substr($date, 0, 4) . '-' . substr($date, 4, 2) . '-' .substr($date, 6, 2);
}

$manager = new MongoDB\Driver\Manager("mongodb://10.249.21.145:27017/?socketTimeoutMS=18000000", ["username" => 'krill', 'password' => 'krill@123#']);
$manager1 = new MongoDB\Driver\Manager("mongodb://10.249.21.142:27017/?socketTimeoutMS=18000000", ["username" => 'krill', 'password' => 'krill@123#']);
$manager_hist = new MongoDB\Driver\Manager("mongodb://192.168.61.198:27017/?socketTimeoutMS=18000000", ["username" => 'krill', 'password' => 'krillU$3r']);
$cbm_manager = new MongoDB\Driver\Manager("mongodb://localhost:27017/?socketTimeoutMS=18000000", ["username" => 'krill', 'password' => 'krillU$3r']);
$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 100);

?>

