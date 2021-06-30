<?php 
$IP = $_GET['info-ip'];
$PORT = $_GET['info-port'];
$USER = $_GET['info-user'];
$PWD = $_GET['info-pass'];
$DB = $_GET['info-db'];
$TRACEID = $_GET['info-traceid'];

if (empty($PWD)) {
  $cmd = "sh mon.sh   -h $IP -P $PORT -u $USER -D $DB --trace-id='$TRACEID' -o report.$TRACEID.html";
} else {
  $cmd = "sh mon.sh   -h $IP -P $PORT -u $USER -D $DB -p $PWD --trace-id='$TRACEID' -o report.$TRACEID.html";
}

$result = shell_exec($cmd);
$rule = "/Report File Generate OK: (report.*\.html)/i";
preg_match($rule, $result, $m);
if (count($m) == 2) {
  header('location:' . $m[1]);
} else {
  echo $cmd;
  echo "<hr />";
  echo $result;
  echo "<p><a href='index.php?info-ip=$IP&info-port=$PORT&info-user=$USER&info-pass=$PWD&info-db=$DB&info-traceid=$TRACEID'>Wrong Parameters. Go Back!</a></p>";
}
?>
