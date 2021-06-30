<?php
$IP = $_GET['info-ip'];
$PORT = $_GET['info-port'];
$USER = $_GET['info-user'];
$PWD = $_GET['info-pass'];
$DB = $_GET['info-db'];
$TRACEID = $_GET['info-traceid'];
?>
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
<link rel="stylesheet" href="___xiaochu_packages___/web/bootstrap4.min.css" >
<link rel="icon" type="image/png" href="favicon.png" />

<script src="___xiaochu_packages___/web/jquery-3.2.1.min.js" ></script>
<script src="___xiaochu_packages___/web/popper.min.js"></script>
<script src="___xiaochu_packages___/web/bootstrap.min.js" ></script>
<script>window.jQuery || alert('请将 report 文件放到 mon.sh 相同目录后再访问。否则无法显示可视化图表。'); </script>
<style>
body{ padding:10px; padding-bottom:50px;}
table {font-family: Consolas,"Courier New",Courier,FreeMono,monospace !important;}
.fixed{ position:fixed; right:20px; bottom:0px; width:200px; height:50px; background-color:#fef8e9; z-index:9999;border-radius:10px;padding:5px;margin: 0 auto;}
h2 { text-decoration:underline;color:blue; cursor:pointer; margin-bottom:20px;}
.diff {display:none;margin-left:20px;padding:5px;background-color:#fef8e9;color:black;border-radius:5px;position:absolute;}
.graph-table tr {padding:0px;line-height:0.4em;}
.graph-table>tr>td, .graph-table>tr>th {padding:0px !important;line-height:0.4em !important;vertical-align:middle !important;}
.graph-table>tr>.lastline {padding:0px !important;line-height:1.4em !important;vertical-align:middle !important;}
.graph-table {font-size:10px;line-height:0.6em;width:1000px;}
*{margin: 0; padding: 0;}
.b {height: 14px;}
.empty { height: 14px; background: rgba(200,200,200,0.2);}
.bar { margin-left:5%; margin-top: 20px; margin-bottom:100px; }
.help {background-color:#fef8e9;width:100%;border-left:6px solid orange;padding:30px 20px;}
.shortcut {color:gray;text-align:right;}
</style>
</head>
<body>

<div class='help'><h1>SQL Monitor Report</h1><p>使用帮助: <a target='_blank' href='https://yuque.antfin-inc.com/xiaochu.yh/doc/rb6pmq'>https://yuque.antfin-inc.com/xiaochu.yh/doc/rb6pmq</a></p></div>

<form action="/test.php" method="get">
  <div class="form-row">
    <div class="form-group col-md-6">
      <label for="ip">IP</label>
      <input type="text" class="form-control" id="ip" name="info-ip" placeholder="10.101.163.79" value="<?php echo $IP; ?>">
    </div>
    <div class="form-group col-md-6">
      <label for="port">Port</label>
      <input type="text" class="form-control" id="port" name="info-port" placeholder="2881" value="<?php echo $PORT; ?>">
    </div>
  </div>
  <div class="form-row">
    <div class="form-group col-md-6">
      <label for="user">UserName</label>
      <input type="text" class="form-control" id="user" name="info-user" placeholder="admin@mysql" value="<?php echo $USER; ?>">
    </div>
    <div class="form-group col-md-4">
      <label for="pass">Password</label>
      <input type="text" class="form-control" id="pass" name="info-pass" placehold="" value="<?php echo $PWD; ?>">
    </div>
    <div class="form-group col-md-2">
      <label for="db">Database</label>
      <input type="text" class="form-control" id="db" name="info-db" placehold="test" value="<?php echo $DB; ?>">
    </div>
  </div>
  <div class="form-group">
    <label for="traceid">TraceID</label>
    <input type="text" class="form-control" id="traceid" name="info-traceid" placeholder="Y4C360A65A34F-0005C5CD5E27A880-0-0" value="<?php echo $TRACEID; ?>">
  </div>
  <button type="submit" class="btn btn-primary">Get Report</button>
</form>

</body>
</html>
