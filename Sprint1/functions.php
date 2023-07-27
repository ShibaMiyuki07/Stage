<?php
function operator($msisdn) {
        $prefix = substr($msisdn,0,2);
        switch ($prefix) {
                case 32: // Orange Mobile
		case 37:
                        $code = 1;
                break;
                case 33: // Airtel
                        $code = 3;
                break;
                case 20: // Telma Fixed
                        $code = 4;
                break;
                case 34: // Telma Mobile
		case 38:
                        $code = 2;
                break;

                case 39: // Blue Line
                        $code = 5;
                break;

        }
        return $code;
}

function getOPId($op) {
        $code  = -1;
        switch ($op) {
                case 32: // Orange Mobile
		case 37:
                        $code = 1;
                break;
                case 33: // Airtel
                        $code = 3;
                break;
                case 20: // Telma Fixed
                        $code = 4;
                break;
                case 34: // Telma Mobile
		case 38:
                        $code = 2;
                break;

                case 39: // Blue Line
                        $code = 5;
                break;

        }
        return $code;
}

function operator_t($msisdn) {
                $code = 99;
                if (strlen($msisdn) < 7) $code = 1;
                if (substr($msisdn,0,3) == 261) {
                        $prefix = substr($msisdn,3,2);
                        if (strlen($msisdn) < 10) $code = 1;
                        else {
                                switch ($prefix) {
                                                case 32: // Orange Mobile
						case 37:
                                                                $code = 1;
                                                break;
                                                case 33: // Airtel
                                                                $code = 3;
                                                break;
                                                case 20: // Telma Fixed
                                                                $code = 4;
                                                break;
                                                case 34: // Telma Mobile
						case 38:
                                                                $code = 2;
                                                break;

                                                case 39: // Blue Line
                                                                $code = 5;
                                                break;
                                }
                        }
                }
        return $code;
}

function country($msisdn) {
 global $country;
  if (substr($msisdn,0,1)=='0') $numero = substr($msisdn,1);
  $code = substr($msisdn,0,4);
  if (strlen($msisdn) >= 9){
          if (isset($country[$code])) {
                          $msisdn = substr($msisdn,4);
          } else {
          $code = substr($msisdn,0,3);
                          if (isset($country[$code])) {
                                          $msisdn = substr($msisdn,3);
                          } else {
                                          $code = substr($msisdn,0,2);
                                          if (isset($country[$code])) {
                                                          $msisdn = substr($msisdn,2);
                                          } else {
                                                          $code = substr($msisdn,0,1);
                                                          if (isset($country[$code])) {
                                                                          $msisdn = substr($msisdn,1);
                                                          } else {
                                                                          $code ='0000';
                                                          }
                                          }
                          }
          }
  }
  else $code = '0000';

  //$num = array('code' => $code, 'msisdn' => $msisdn);
  return $code;
}

function slot($date) {
  $hour = date('H',$date);
  $minute = date('i', $date);
  if ($minute <15) {
        $minute = '00';
  } elseif ($minute <30) {
        $minute = '15';
  } elseif ($minute <45) {
        $minute = '30';
  } else {
        $minute = '45';
  }
  return $hour . ':' . $minute . ':00';
}


