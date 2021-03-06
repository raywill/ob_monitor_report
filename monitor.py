#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import datetime;
import time;
import getopt

import MySQLdb
# prettytable https://cloud.tencent.com/developer/article/1603833
from prettytable import PrettyTable
from prettytable import from_db_cursor
from prettytable import RANDOM
from prettytable import DEFAULT

###################################################################################################
###################################################################################################
###################################################################################################
t = time.localtime(time.time())
version = "2.0"
tenantMode = "mysql"
sysDB = 'oceanbase'
reportFileName="report." + time.strftime("%Y%m%d%H%M%S", t) + ".html"
host="127.0.0.1"
port=2828
user="admin@mysql"
database="test"
password=""
searchTag =None
sqlFile=None
sqlTraceId=None
enableDumpDb = False
enableFastDump = False


reload(sys)
sys.setdefaultencoding('utf8')

def print_schema(cursor, sql_explain_result, enableDumpDb):
  schemas = "";
  valid_words = []
  if enableDumpDb:
    words = [w.strip(',') for w in ("%s" % sql_explain_result).split() if not ("[" in w or "=" in w or "|" in w or "(" in w or "--" in w or "]" in w or ")" in w or "*" in w or "/" in w or "%" in w or "'" in w or "-" in w or w.isdigit())]
    for t in words:
      if t in valid_words:
        continue
      valid_words.append(t)

    for t in valid_words:
      try:
        cursor.execute("show create table %s" % (t))
        data = cursor.fetchone()
        schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % (data[1])
      except Exception as e:
        #print(repr(e))
        pass

  cursor.execute("show variables like '%parallel%'");
  s = from_db_cursor(cursor)
  s.align = 'l'
  schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s

  cursor.execute("show variables");
  s = from_db_cursor(cursor)
  s.align = 'l'
  schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s

  cursor.execute("show parameters");
  s = from_db_cursor(cursor)
  s.align = 'l'
  schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s

  report("<div><h2 id='schema_anchor'>SCHEMA ??????</h2><div id='schema' style='display: none'>" + schemas + "</div></div>")


def print_pre(s):
  pre = '''<pre style='margin:20px;border:1px solid gray;'>%s</pre>''' % (s)
  report(pre)

def print_header():
  header = '''
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
<link rel="stylesheet" href="___xiaochu_packages___/web/bootstrap.min.css" >
<script src="___xiaochu_packages___/web/jquery-3.2.1.min.js" ></script>
<script src="___xiaochu_packages___/web/popper.min.js"></script>
<script src="___xiaochu_packages___/web/bootstrap.min.js" ></script>
<script>window.jQuery || alert('?????? report ???????????? mon.sh ???????????????????????????????????????????????????????????????'); </script>
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
<div class='help'><h1>SQL Monitor Report</h1><p>????????????: @??????</p></div>
<script>
$(function() {
  $('table').addClass('table-bordered');
  $('.v table').addClass('table table-bordered table-striped');
  $('#schema_anchor').click(function() {
    $('#schema').toggle();
  });
  $('#agg_table_anchor').click(function() {
    $('#agg_table').toggle();
  });
  $('#svr_agg_table_anchor').click(function() {
    $('#svr_agg_table').toggle();
  });
  $('#detail_table_anchor').click(function() {
    $('#detail_table').toggle();
  });
  $('#sql_audit_table_anchor').click(function() {
    $('#sql_audit_table').toggle();
  });
  setTimeout(function() {
    $('#debug').hide();
  }, 30*1000);

});

//?????????????????????
function getSafeColor(n) {
  var base = ['00','33','66','99','CC','FF'];     //???????????????
  var len = base.length;                          //???????????????
  var bg = new Array();                           //???????????????
  var random = Math.ceil( n * 17 % 200 + 13);    //??????1-216??????????????????
  var res;
  for(var r = 0; r  <  len; r++){
    for(var g = 0; g  <  len; g++){
      for(var b = 0; b  <  len; b++){
        bg.push('#'+base[r].toString()+base[g].toString()+base[b].toString());
      }
    };
  };
  for(var i=0;i < bg.length;i++){
    res =  bg[random];
  }
  return res;
}

var colors = [];
for (var n = 0; n < 1000; ++n) {
  colors[n] = getSafeColor(n);
}

function padding(n) {
  return "";
}

function generate_graph(type, serial, topnode) {
  var max = 0;
  var min = 999999999999999;
  for (var i = 0; i < serial.length; ++i) {
    if (serial[i].start > 0) {
      max = Math.max(max, serial[i].end);
      min = Math.min(min, serial[i].start);
    }
  }

  var total = max - min;

  // normalize

  for (var i = 0; i < serial.length; ++i) {
     if (serial[i].start > 0) {
        serial[i].start_relative = serial[i].start - min;
        serial[i].length = serial[i].end - serial[i].start;

        serial[i].a = Math.round(serial[i].start_relative * 100 / total);
        serial[i].b = Math.max(0.1, Math.round(serial[i].length * 100 / total));
        serial[i].c = Math.round(100 - serial[i].a - serial[i].b);
     }
  }
   console.log(topnode, "my", serial);


  var c1 = (undefined == serial[0] || serial[0].tag == 'op') ? '??????ID' : '?????????';
  var ext_header = "";
  var ext_footer = "";
  switch(type) {
  case "dfo":
	ext_header = "<td width='5%'>??????</td>";
	ext_footer = "<td></td>";
	break;
  case "detail":
 	ext_header = "<td width='5%'>RESCAN</td>";
	ext_footer = "<td></td>";
	break;
  default:
	break;
  }
  var table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td><td>??????</td>" + ext_header + "<td>??????</td><td>?????????????????????</td></tr>";
  for (var i = 0; i < serial.length - 1; ++i) {
    var ext_data = "";
    ext_data += (serial[i].est_rows === undefined ? "" : "<td>" + serial[i].est_rows + "</td>");
    ext_data += (serial[i].rescan === undefined ? "" : "<td>" + serial[i].rescan + "</td>");
    var row = "<tr><td width='5%'>" +  serial[i].tid + "</td><td width='10%'>" + "&nbsp;".repeat(serial[i].depth) + serial[i].op + "(" + serial[i].opid + ")</td>" + ext_data + "<td width='5%' style='text-align:right'>" +  serial[i].rows + "</td>";
    if (serial[i].start > 0) {
      row += "<td width='80%'><div tabindex='" + i + "' class='b graphrow' data-toggle='popover' data-trigger='focus' data-placement='bottom' data-content='" + JSON.stringify(serial[i]).replace(/,/g,'\\n') + "' style='margin-left:" + serial[i].a + "%;width:" + serial[i].b + "%;background-color:" + colors[serial[i].opid] + "' title='" + serial[i].op + "(" + serial[i].diff + "s, " + serial[i].svr_ip +")'><span class='diff'>" +  serial[i].op  + "(" + serial[i].opid + ")" + ' ' + serial[i].diff + "s</span></div></td>";
    } else {
      row += "<td><div class='empty' style='width:100%;'></div></td>";
    }
    row += "</tr>";
    table += row;
  }
  table += "<tr><td class='lastline'><button style='line-height:1.4em' class='enlarge'>??????</button></td><td></td>"+ext_footer+"<td>?????????</td><td class='b' style='text-align:center;'>" + (Math.round(total * 1000000) / 1000000.0) + "s</td></tr></table>"
  topnode.get(0).innerHTML = table;
}
</script>
'''
  with open(reportFileName, 'w') as f:
    f.write(header)

def print_detail_graph_data(ident, cursor, title=''):
  base = 1
  data = "<script> var %s = [" % ident
  for item in cursor:
    start = (0 if None == item[base+14] else item[base+14])
    end = (0 if None == item[base+15] else item[base+15])
    # start = (0 if None == item[14] else item[14])
    # end = (0 if None == item[15] else item[15])
    rows = (0 if None == item[base+5] else item[base+5])
    data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d, tag:'op', depth:%d, rescan:%d, svr_ip:'%s'}," % (start, end, end-start, item[4], item[5], item[3], rows, item[base-1],item[7],item[1])
  data = data + "{start:0}];</script>"
  data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
  report(data)

#dfo
def print_dfo_agg_graph_data(cursor, title=''):
  base = 2
  data = "<script> var agg_serial = ["
  for item in cursor:
    start = (0 if None == item[base+9] else item[base+9])
    end = (0 if None == item[base+10] else item[base+10])
    rows = (0 if None == item[base+17] else item[base+17])
    est_rows = (0 if None == item[base-2] else item[base-2])
    data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, tag:'dfo', depth:%d}," % (start,end, end-start, item[base+0], item[base+1], item[base+2],rows, est_rows, item[base-1])
  data = data + "{start:0}];"
  data = data + "</script><p>%s</p><div class='bar' id='agg_serial'></div>" % (title);
  report(data)

def print_dfo_sched_agg_graph_data(cursor, title=''):
  base = 2
  data = "<script> var agg_sched_serial = ["
  for item in cursor:
    start = (0 if None == item[base+7] else item[base+7])
    end = (0 if None == item[base+8] else item[base+8])
    rows = (0 if None == item[base+17] else item[base+17])
    est_rows = (0 if None == item[base-2] else item[base-2])
    data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, tag:'dfo', depth:%d}," % (start,end, end-start, item[base+0], item[base+1], item[base+2],rows, est_rows, item[base-1])
  data = data + "{start:0}];"
  data = data + "</script><p>%s</p><div class='bar' id='agg_sched_serial'></div>" % (title);
  report(data)


#sqc
def print_svr_agg_graph_data(ident, cursor, title=''):
  base = 1
  data = "<script> var %s = [" % ident
  for item in cursor:
    start = (0 if None == item[base+9] else item[base+9])
    end = (0 if None == item[base+10] else item[base+10])
    rows = (0 if None == item[base+17] else item[base+17])
    data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',svr:'%s',rows:%d, tag:'sqc', depth:%d}," % (start,end,end-start, item[base+0], item[base+1], item[base+2], item[base+19]+':'+str(item[base+20]),rows, item[base-1])
  data = data + "{start:0}];</script>"
  data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
  report(data)

def print_fast_preview():
  content = '''
<script>
generate_graph("dfo", agg_serial, $('#agg_serial'));
generate_graph("dfo", agg_sched_serial, $('#agg_sched_serial'));
generate_graph("sqc", svr_agg_serial_v1, $('#svr_agg_serial_v1'));
generate_graph("sqc", svr_agg_serial_v2, $('#svr_agg_serial_v2'));
</script>
'''
  report(content)

def print_footer():
  footer = '''
<script>
generate_graph("detail", detail_serial_v1, $('#detail_serial_v1'));
generate_graph("detail", detail_serial_v2, $('#detail_serial_v2'));

$(function () {
  $('.b').popover({ trigger: 'focus' })
  $('.graphrow').mouseover(function(){
    $(this).find('span').show();
  });
  $('.graphrow').mouseleave(function(){
    $(this).find('span').hide();
  });
})

$(function() {
  $('.enlarge').click(function(e) {
    if ($(e.target).parents('.graph-table').css('width') == '1000px') {
      $(e.target).parents('.graph-table').css('width', '3000px');
      $('body').css('min-width', '4000px');
    } else {
      $(e.target).parents('.graph-table').css('width', '1000px');
      $('body').css('min-width', '100%');
    }
    console.log($(e.target).parents('.graph-table').css('width'));
  });
});
</script>

<div id='debug' class="fixed">???????????? @??????</div>
</body>
</html>
'''
  report(footer)

def report(str):
  with open(reportFileName, 'a') as f:
    f.write(str)



try:
  opts, args = getopt.getopt(sys.argv[1:],"h:P:p:D:u:o:v", ["pattern=", "sql=", "trace-id=", "trace_id=", "dump-db", "fast"])
except getopt.GetoptError:
  print('invalid parameter. example:');
  print('%s -h 127.1 -P 2888 -uroot@sys -padmin -Dtpch_db --pattern=TPCH_ -o report_01.html --sql=1.sql' % (sys.argv[0]))
  sys.exit(2)
for opt, arg in opts:
  if opt in ("-h"):
    host=arg
  elif opt in ("-P"):
    port= int(arg)
  elif opt in ("-u"):
    user=arg
  elif opt in ("-p"):
    password = arg
  elif opt in ("-D"):
    database = arg
  elif opt in ("--pattern"):
    searchTag= arg
  elif opt in ("-o"):
    reportFileName = arg.replace("date",  time.strftime("%Y-%m-%d", t)).replace("time",  time.strftime("%H-%M-%S", t))
  elif opt in ("--sql"):
    sqlFile = arg
  elif opt in ("--trace-id", "--trace_id"):
    sqlTraceId= arg
  elif opt in ("--dump-db"):
    enableDumpDb = True
  elif opt in ("--fast"):
    enableFastDump = True
  elif opt in ("-v"):
    print("version: 1.0.0")
    sys.exit(0)

if sqlTraceId != None and searchTag != None:
  print ("should not use --trace-id and --pattern the same time")
  sys.exit(2)

if sqlTraceId != None and sqlFile != None:
  print ("should not use --trace-id and --sql the same time")
  sys.exit(2)

if sqlTraceId == None and searchTag == None:
  print ("should specify --trace-id or --pattern to tell me which sql monitor info to dump")
  sys.exit(2)

print ((host,port,user,password,database,searchTag, reportFileName, sqlFile, sqlTraceId))

# ?????????????????????
try:
  db = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=database)
except Exception as e:
  print(repr(e))
  print("fail connect server")
  sys.exit(1)

# ??????cursor()????????????????????????
cursor = db.cursor()

# ??????execute????????????SQL??????
cursor.execute("SELECT 'Connect OceanBase OK!' FROM DUAL")
data = cursor.fetchone()
print("Welcome : %s " % (data))

# ?????????????????????????????? MySQL
try:
  cursor.execute("select version()");
  data = cursor.fetchone()
  print("Database version : %s " % (data))
except:
  print("Oracle mode detected");
  tenantMode = "oracle"
  sysDB = 'SYS'

tpch_sql = '''
SELECT  /*+   TPCH_Q01 parallel(100) USE_PX */
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date'1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
'''
tpch_sql = "select /*+ parallel(2) TPCH_Q01 markxx */ * from nation a, nation b, nation c where a.n_nationkey = c.n_nationkey"

if sqlFile != None:
  try:
    with open(sqlFile, 'r') as f:
      tpch_sql = f.read()
      dt1 = datetime.datetime.now();
      cursor.execute(tpch_sql)
      data = cursor.fetchall()
      dt2 = datetime.datetime.now()
      delta = dt2 - dt1
      seconds = delta.total_seconds() # microseconds
      print ("SQL time: %s" % (seconds));
  except Exception as e:
    print(repr(e))
    print ("fail open sql file %s" % (sqlFile))
    sys.exit(2);

#print(data)

if searchTag != None:
  if tenantMode == 'mysql':
    audit_sql = "select /*+ sql_audit */ trace_id,query_sql, RETURN_ROWS, from_unixtime(REQUEST_TIME/1000000) REQUEST_TIME, from_unixtime((REQUEST_TIME + ELAPSED_TIME)/1000000) REQUEST_END_TIME, ELAPSED_TIME, TENANT_NAME, user_name, db_name, plan_id, tenant_id, version() mysql_version, svr_ip, svr_port from oceanbase.gv$sql_audit where query_sql like '%%%s%%' and query_sql not like 'explain%%' and query_sql not like 'show%%' and query_sql not like '%%sql_audit%%' order by REQUEST_TIME desc limit 1" % (searchTag)
  else:
    audit_sql = "select * from (select /*+ sql_audit */ trace_id,query_sql, RETURN_ROWS, REQUEST_TIME, REQUEST_TIME + ELAPSED_TIME REQUEST_END_TIME, ELAPSED_TIME,TENANT_NAME, user_name, db_name , plan_id, tenant_id, banner oracle_version, svr_ip, svr_port from sys.gv$sql_audit, V$VERSION  where query_sql like '%%%s%%' and query_sql not like 'explain%%' and query_sql not like 'show%%' and query_sql not like '%%sql_audit%%' order by REQUEST_TIME desc) where rownum < 2" % (searchTag)
elif sqlTraceId != None:
  if tenantMode == 'mysql':
    audit_sql = "select /*+ sql_audit */ trace_id,query_sql, RETURN_ROWS, from_unixtime(REQUEST_TIME/1000000) REQUEST_TIME, from_unixtime((REQUEST_TIME + ELAPSED_TIME)/1000000) REQUEST_END_TIME, ELAPSED_TIME, TENANT_NAME, user_name, db_name,  plan_id, tenant_id, version() mysql_version, svr_ip, svr_port from oceanbase.gv$sql_audit where query_sql != '' and trace_id='%s' order by REQUEST_TIME desc limit 1" % (sqlTraceId)
  else:
    audit_sql = "select * from (select /*+ sql_audit */ trace_id,query_sql, RETURN_ROWS, REQUEST_TIME, REQUEST_TIME + ELAPSED_TIME REQUEST_END_TIME, ELAPSED_TIME, TENANT_NAME, user_name, db_name , plan_id, tenant_id,banner oracle_version, svr_ip, svr_port from sys.gv$sql_audit, V$VERSION  where length(query_sql)>4 and trace_id='%s' order by REQUEST_TIME desc) where rownum < 2" % (sqlTraceId)

cursor.execute(audit_sql);
trace = cursor.fetchone()
if (None == trace):
  if None != searchTag:
    print("No match result for pattern '%s'" % searchTag)
  else:
    print("No match result for trace-id %s" % sqlTraceId)
  sys.exit(2)

trace_id = trace[0]
sql = trace[1]
user_sql = trace[1]
db_name = trace[8]
plan_id = trace[9]
tenant_id = trace[10]
svr_ip = trace[12]
svr_port = trace[13]
print("TraceID : %s " % (trace_id))
print("SQL : %s " % (sql))
print("DB: %s " % (db_name))
print("PLAN_ID: %s " % (plan_id))
print("TENANT_ID: %s " % (tenant_id))
print("Dumping sql info...")
explain_sql = "explain %s" % (sql);
go_parallel_query_sql= "set _force_parallel_query_dop = 2;" if tenantMode == 'mysql' else "alter session force parallel query parallel 2;"
go_parallel_dml_sql= "set _force_parallel_dml_dop=2;" if tenantMode == 'mysql' else "alter session force parallel dml parallel 2;"

audit_items_mysql ='`SVR_IP`,`SVR_PORT`,`REQUEST_ID`,`SQL_EXEC_ID`,`TRACE_ID`,`SID`,`CLIENT_IP`,`CLIENT_PORT`,`TENANT_ID`,`EFFECTIVE_TENANT_ID`,`TENANT_NAME`,`USER_ID`,`USER_NAME`,`USER_CLIENT_IP`,`DB_ID`,`DB_NAME`,`SQL_ID`,`QUERY_SQL`,`PLAN_ID`,`AFFECTED_ROWS`,`RETURN_ROWS`,`PARTITION_CNT`,`RET_CODE`,`QC_ID`,`DFO_ID`,`SQC_ID`,`WORKER_ID`,`EVENT`,`P1TEXT`,`P1`,`P2TEXT`,`P2`,`P3TEXT`,`P3`,`LEVEL`,`WAIT_CLASS_ID`,`WAIT_CLASS`,`STATE`,`WAIT_TIME_MICRO`,`TOTAL_WAIT_TIME_MICRO`,`TOTAL_WAITS`,`RPC_COUNT`,`PLAN_TYPE`,`IS_INNER_SQL`,`IS_EXECUTOR_RPC`,`IS_HIT_PLAN`,`REQUEST_TIME`,`ELAPSED_TIME`,`NET_TIME`,`NET_WAIT_TIME`,`QUEUE_TIME`,`DECODE_TIME`,`GET_PLAN_TIME`,`EXECUTE_TIME`,`APPLICATION_WAIT_TIME`,`CONCURRENCY_WAIT_TIME`,`USER_IO_WAIT_TIME`,`SCHEDULE_TIME`,`ROW_CACHE_HIT`,`BLOOM_FILTER_CACHE_HIT`,`BLOCK_CACHE_HIT`,`BLOCK_INDEX_CACHE_HIT`,`DISK_READS`,`RETRY_CNT`,`TABLE_SCAN`,`CONSISTENCY_LEVEL`,`MEMSTORE_READ_ROW_COUNT`,`SSSTORE_READ_ROW_COUNT`,`REQUEST_MEMORY_USED`,`EXPECTED_WORKER_COUNT`,`USED_WORKER_COUNT`,`PS_STMT_ID`,`TRANSACTION_HASH`,`REQUEST_TYPE`,`IS_BATCHED_MULTI_STMT`,`OB_TRACE_INFO`,`PLAN_HASH`'
audit_items_oracle ='"SVR_IP","SVR_PORT","REQUEST_ID","SQL_EXEC_ID","TRACE_ID","SID","CLIENT_IP","CLIENT_PORT","TENANT_ID","EFFECTIVE_TENANT_ID","TENANT_NAME","USER_ID","USER_NAME","USER_CLIENT_IP","DB_ID","DB_NAME","SQL_ID","QUERY_SQL","PLAN_ID","AFFECTED_ROWS","RETURN_ROWS","PARTITION_CNT","RET_CODE","QC_ID","DFO_ID","SQC_ID","WORKER_ID","EVENT","P1TEXT","P1","P2TEXT","P2","P3TEXT","P3","LEVEL","WAIT_CLASS_ID","WAIT_CLASS","STATE","WAIT_TIME_MICRO","TOTAL_WAIT_TIME_MICRO","TOTAL_WAITS","RPC_COUNT","PLAN_TYPE","IS_INNER_SQL","IS_EXECUTOR_RPC","IS_HIT_PLAN","REQUEST_TIME","ELAPSED_TIME","NET_TIME","NET_WAIT_TIME","QUEUE_TIME","DECODE_TIME","GET_PLAN_TIME","EXECUTE_TIME","APPLICATION_WAIT_TIME","CONCURRENCY_WAIT_TIME","USER_IO_WAIT_TIME","SCHEDULE_TIME","ROW_CACHE_HIT","BLOOM_FILTER_CACHE_HIT","BLOCK_CACHE_HIT","BLOCK_INDEX_CACHE_HIT","DISK_READS","RETRY_CNT","TABLE_SCAN","CONSISTENCY_LEVEL","MEMSTORE_READ_ROW_COUNT","SSSTORE_READ_ROW_COUNT","REQUEST_MEMORY_USED","EXPECTED_WORKER_COUNT","USED_WORKER_COUNT","PS_STMT_ID","TRANSACTION_HASH","REQUEST_TYPE","IS_BATCHED_MULTI_STMT","OB_TRACE_INFO","PLAN_HASH"'


if tenantMode == 'mysql':
  full_audit_sql = "select /*+ sql_audit */ %s from oceanbase.gv$sql_audit where trace_id = '%s' AND client_ip IS NOT NULL ORDER BY QUERY_SQL ASC, REQUEST_ID" % (audit_items_mysql, trace_id)
else:
  full_audit_sql = "select /*+ sql_audit */ %s from sys.gv$sql_audit where trace_id = '%s' AND  length(client_ip) > 4 ORDER BY  REQUEST_ID" % (audit_items_oracle, trace_id)


if tenantMode == 'mysql':
  plan_explain_sql = "select * from oceanbase.gv$plan_cache_plan_explain where tenant_id = %s and plan_id = %s  and ip = '%s' and port = %s" % (tenant_id, plan_id, svr_ip, svr_port)
else:
  plan_explain_sql = "select * from gv$plan_cache_plan_explain where tenant_id = %s and plan_id = %s  and svr_ip = '%s' and svr_port = %s" % (tenant_id, plan_id, svr_ip, svr_port)

sql_plan_monitor_dfo_agg_oracle = '''
SELECT
  AVG("ROWS") EST_ROWS,
  plan_monitor.PLAN_DEPTH,
  plan_monitor.PLAN_LINE_ID,
  PLAN_OPERATION,
  COUNT(*) PARALLEL,
  MIN(FIRST_REFRESH_TIME) MIN_FIRST_REFRESH_TIME,
  MAX(LAST_REFRESH_TIME) MAX_LAST_REFRESH_TIME,
  MIN(FIRST_CHANGE_TIME) MIN_FIRST_CHANGE_TIME,
  MAX(LAST_CHANGE_TIME) MAX_LAST_CHANGE_TIME,
  MIN(FIRST_REFRESH_TS) MIN_FIRST_REFRESH_TS,
  MAX(LAST_REFRESH_TS) MAX_LAST_REFRESH_TS,
  MIN(FIRST_CHANGE_TS) MIN_FIRST_CHANGE_TS,
  MAX(LAST_CHANGE_TS) MAX_LAST_CHANGE_TS,
  AVG(REFRESH_TS) AVG_REFRESH_TIME,
  MAX(REFRESH_TS) MAX_REFRESH_TIME,
  MIN(REFRESH_TS) MIN_REFRESH_TIME,
  AVG(CHANGE_TS) AVG_CHANGE_TIME,
  MAX(CHANGE_TS) MAX_CHANGE_TIME,
  MIN(CHANGE_TS) MIN_CHANGE_TIME,
  SUM(OUTPUT_ROWS) TOTAL_OUTPUT_ROWS,
  SUM(STARTS) TOTAL_RESCAN_TIMES,
  MAX(OTHERSTAT_1_VALUE) MAX_STAT_1,
  MIN(OTHERSTAT_1_VALUE) MIN_STAT_1,
  AVG(OTHERSTAT_1_VALUE) AVG_STAT_1,
  MAX(OTHERSTAT_2_VALUE) MAX_STAT_2,
  MIN(OTHERSTAT_2_VALUE) MIN_STAT_2,
  AVG(OTHERSTAT_2_VALUE) AVG_STAT_2,
  MAX(OTHERSTAT_3_VALUE) MAX_STAT_3,
  MIN(OTHERSTAT_3_VALUE) MIN_STAT_3,
  AVG(OTHERSTAT_3_VALUE) AVG_STAT_3,
  MAX(OTHERSTAT_4_VALUE) MAX_STAT_4,
  MIN(OTHERSTAT_4_VALUE) MIN_STAT_4,
  AVG(OTHERSTAT_4_VALUE) AVG_STAT_4,
  MAX(OTHERSTAT_5_VALUE) MAX_STAT_5,
  MIN(OTHERSTAT_5_VALUE) MIN_STAT_5,
  AVG(OTHERSTAT_5_VALUE) AVG_STAT_5,
  MAX(OTHERSTAT_6_VALUE) MAX_STAT_6,
  MIN(OTHERSTAT_6_VALUE) MIN_STAT_6,
  AVG(OTHERSTAT_6_VALUE) AVG_STAT_6
FROM
(
select
  PLAN_DEPTH,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  OUTPUT_ROWS,
  STARTS,
  FIRST_REFRESH_TIME,
  LAST_REFRESH_TIME,
  FIRST_CHANGE_TIME,
  LAST_CHANGE_TIME,
  OTHERSTAT_1_VALUE,
  OTHERSTAT_2_VALUE,
  OTHERSTAT_3_VALUE,
  OTHERSTAT_4_VALUE,
  OTHERSTAT_5_VALUE,
  OTHERSTAT_6_VALUE,
  24 * 3600 * extract(day FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_REFRESH_TS,
  24 * 3600 * extract(day FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 3600 * extract(hour FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 60 * extract(minute FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + extract(second FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 3600 * extract(hour FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 60 * extract(minute FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + extract(second FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) CHANGE_TS
from
  sys.gv$sql_plan_monitor
  where
    trace_id = '%s'

) plan_monitor
LEFT JOIN
(
 SELECT "ROWS", PLAN_LINE_ID FROM sys.v$plan_cache_plan_explain WHERE plan_id = %s AND tenant_id = %s
) plan_explain
ON
  plan_monitor.PLAN_LINE_ID = plan_explain.PLAN_LINE_ID
GROUP BY
  plan_monitor.PLAN_LINE_ID, PLAN_OPERATION, plan_monitor.PLAN_DEPTH
ORDER BY
  plan_monitor.PLAN_LINE_ID ASC;
''' % (trace_id, plan_id, tenant_id)

sql_plan_monitor_dfo_agg = '''
select
  AVG(ROWS) EST_ROWS,
  plan_monitor.PLAN_DEPTH PLAN_DEPTH,
  plan_monitor.PLAN_LINE_ID PLAN_LINE_ID,
  PLAN_OPERATION,
  COUNT(*) PARALLEL,
  MIN(FIRST_REFRESH_TIME) MIN_FIRST_REFRESH_TIME,
  MAX(LAST_REFRESH_TIME) MAX_LAST_REFRESH_TIME,
  MIN(FIRST_CHANGE_TIME) MIN_FIRST_CHANGE_TIME,
  MAX(LAST_CHANGE_TIME) MAX_LAST_CHANGE_TIME,
  UNIX_TIMESTAMP(MIN(FIRST_REFRESH_TIME)) MIN_FIRST_REFRESH_TS,
  UNIX_TIMESTAMP(MAX(LAST_REFRESH_TIME)) MAX_LAST_REFRESH_TS,
  UNIX_TIMESTAMP(MIN(FIRST_CHANGE_TIME)) MIN_FIRST_CHANGE_TS,
  UNIX_TIMESTAMP(MAX(LAST_CHANGE_TIME)) MAX_LAST_CHANGE_TS,
  AVG(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) AVG_REFRESH_TIME,
  MAX(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) MAX_REFRESH_TIME,
  MIN(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) MIN_REFRESH_TIME,
  AVG(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) AVG_CHANGE_TIME,
  MAX(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) MAX_CHANGE_TIME,
  MIN(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) MIN_CHANGE_TIME,
  SUM(OUTPUT_ROWS) TOTAL_OUTPUT_ROWS,
  SUM(STARTS) TOTAL_RESCAN_TIMES,
  MAX(OTHERSTAT_1_VALUE) MAX_STAT_1,
  MIN(OTHERSTAT_1_VALUE) MIN_STAT_1,
  AVG(OTHERSTAT_1_VALUE) AVG_STAT_1,
  MAX(OTHERSTAT_2_VALUE) MAX_STAT_2,
  MIN(OTHERSTAT_2_VALUE) MIN_STAT_2,
  AVG(OTHERSTAT_2_VALUE) AVG_STAT_2,
  MAX(OTHERSTAT_3_VALUE) MAX_STAT_3,
  MIN(OTHERSTAT_3_VALUE) MIN_STAT_3,
  AVG(OTHERSTAT_3_VALUE) AVG_STAT_3,
  MAX(OTHERSTAT_4_VALUE) MAX_STAT_4,
  MIN(OTHERSTAT_4_VALUE) MIN_STAT_4,
  AVG(OTHERSTAT_4_VALUE) AVG_STAT_4,
  MAX(OTHERSTAT_5_VALUE) MAX_STAT_5,
  MIN(OTHERSTAT_5_VALUE) MIN_STAT_5,
  AVG(OTHERSTAT_5_VALUE) AVG_STAT_5,
  MAX(OTHERSTAT_6_VALUE) MAX_STAT_6,
  MIN(OTHERSTAT_6_VALUE) MIN_STAT_6,
  AVG(OTHERSTAT_6_VALUE) AVG_STAT_6
from
(
  select * FROM oceanbase.gv$sql_plan_monitor
where
  trace_id = '%s'
) plan_monitor
LEFT JOIN
(
 SELECT ROWS, PLAN_LINE_ID FROM oceanbase.v$plan_cache_plan_explain WHERE plan_id = %s AND tenant_id = %s
) plan_explain
ON
  plan_monitor.PLAN_LINE_ID = plan_explain.PLAN_LINE_ID
GROUP BY
  plan_monitor.PLAN_LINE_ID, plan_monitor.PLAN_OPERATION
ORDER BY
  plan_monitor.PLAN_LINE_ID ASC
''' % (trace_id, plan_id, tenant_id)

sql_plan_monitor_dfo_agg = sql_plan_monitor_dfo_agg if tenantMode == 'mysql' else sql_plan_monitor_dfo_agg_oracle


sql_plan_monitor_svr_agg_oracle_template = '''
SELECT
  PLAN_DEPTH,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  COUNT(*) PARALLEL,
  MIN(FIRST_REFRESH_TIME) MIN_FIRST_REFRESH_TIME,
  MAX(LAST_REFRESH_TIME) MAX_LAST_REFRESH_TIME,
  MIN(FIRST_CHANGE_TIME) MIN_FIRST_CHANGE_TIME,
  MAX(LAST_CHANGE_TIME) MAX_LAST_CHANGE_TIME,
  MIN(FIRST_REFRESH_TS) MIN_FIRST_REFRESH_TS,
  MAX(LAST_REFRESH_TS) MAX_LAST_REFRESH_TS,
  MIN(FIRST_CHANGE_TS) MIN_FIRST_CHANGE_TS,
  MAX(LAST_CHANGE_TS) MAX_LAST_CHANGE_TS,
  AVG(REFRESH_TS) AVG_REFRESH_TIME,
  MAX(REFRESH_TS) MAX_REFRESH_TIME,
  MIN(REFRESH_TS) MIN_REFRESH_TIME,
  AVG(CHANGE_TS) AVG_CHANGE_TIME,
  MAX(CHANGE_TS) MAX_CHANGE_TIME,
  MIN(CHANGE_TS) MIN_CHANGE_TIME,
  SUM(OUTPUT_ROWS) TOTAL_OUTPUT_ROWS,
  SUM(STARTS) TOTAL_RESCAN_TIMES,
  SVR_IP,
  SVR_PORT,
  MAX(OTHERSTAT_1_VALUE) MAX_STAT_1,
  MIN(OTHERSTAT_1_VALUE) MIN_STAT_1,
  AVG(OTHERSTAT_1_VALUE) AVG_STAT_1,
  MAX(OTHERSTAT_2_VALUE) MAX_STAT_2,
  MIN(OTHERSTAT_2_VALUE) MIN_STAT_2,
  AVG(OTHERSTAT_2_VALUE) AVG_STAT_2,
  MAX(OTHERSTAT_3_VALUE) MAX_STAT_3,
  MIN(OTHERSTAT_3_VALUE) MIN_STAT_3,
  AVG(OTHERSTAT_3_VALUE) AVG_STAT_3,
  MAX(OTHERSTAT_4_VALUE) MAX_STAT_4,
  MIN(OTHERSTAT_4_VALUE) MIN_STAT_4,
  AVG(OTHERSTAT_4_VALUE) AVG_STAT_4,
  MAX(OTHERSTAT_5_VALUE) MAX_STAT_5,
  MIN(OTHERSTAT_5_VALUE) MIN_STAT_5,
  AVG(OTHERSTAT_5_VALUE) AVG_STAT_5,
  MAX(OTHERSTAT_6_VALUE) MAX_STAT_6,
  MIN(OTHERSTAT_6_VALUE) MIN_STAT_6,
  AVG(OTHERSTAT_6_VALUE) AVG_STAT_6
FROM
(
select
  PLAN_DEPTH,
  SVR_IP,
  SVR_PORT,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  OUTPUT_ROWS,
  STARTS,
  FIRST_REFRESH_TIME,
  LAST_REFRESH_TIME,
  FIRST_CHANGE_TIME,
  LAST_CHANGE_TIME,
  OTHERSTAT_1_VALUE,
  OTHERSTAT_2_VALUE,
  OTHERSTAT_3_VALUE,
  OTHERSTAT_4_VALUE,
  OTHERSTAT_5_VALUE,
  OTHERSTAT_6_VALUE,
  24 * 3600 * extract(day FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_REFRESH_TS,
  24 * 3600 * extract(day FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 3600 * extract(hour FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 60 * extract(minute FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + extract(second FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 3600 * extract(hour FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 60 * extract(minute FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + extract(second FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) CHANGE_TS
from
  sys.gv$sql_plan_monitor
  where
    trace_id = '{trace_id}'
) tmp
GROUP BY
  PLAN_LINE_ID,PLAN_OPERATION,PLAN_DEPTH,SVR_IP,SVR_PORT
ORDER BY
  {order_by}
'''

sql_plan_monitor_svr_agg_template = '''
select
  PLAN_DEPTH,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  COUNT(*) PARALLEL,
  MIN(FIRST_REFRESH_TIME) MIN_FIRST_REFRESH_TIME,
  MAX(LAST_REFRESH_TIME) MAX_LAST_REFRESH_TIME,
  MIN(FIRST_CHANGE_TIME) MIN_FIRST_CHANGE_TIME,
  MAX(LAST_CHANGE_TIME) MAX_LAST_CHANGE_TIME,
  UNIX_TIMESTAMP(MIN(FIRST_REFRESH_TIME)) MIN_FIRST_REFRESH_TS,
  UNIX_TIMESTAMP(MAX(LAST_REFRESH_TIME)) MAX_LAST_REFRESH_TS,
  UNIX_TIMESTAMP(MIN(FIRST_CHANGE_TIME)) MIN_FIRST_CHANGE_TS,
  UNIX_TIMESTAMP(MAX(LAST_CHANGE_TIME)) MAX_LAST_CHANGE_TS,
  AVG(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) AVG_REFRESH_TIME,
  MAX(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) MAX_REFRESH_TIME,
  MIN(TIMESTAMPDIFF(MICROSECOND, FIRST_REFRESH_TIME, LAST_REFRESH_TIME)) MIN_REFRESH_TIME,
  AVG(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) AVG_CHANGE_TIME,
  MAX(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) MAX_CHANGE_TIME,
  MIN(TIMESTAMPDIFF(MICROSECOND, FIRST_CHANGE_TIME, LAST_CHANGE_TIME)) MIN_CHANGE_TIME,
  SUM(OUTPUT_ROWS) TOTAL_OUTPUT_ROWS,
  SUM(STARTS) TOTAL_RESCAN_TIMES,
  SVR_IP,
  SVR_PORT,
  MAX(OTHERSTAT_1_VALUE) MAX_STAT_1,
  MIN(OTHERSTAT_1_VALUE) MIN_STAT_1,
  AVG(OTHERSTAT_1_VALUE) AVG_STAT_1,
  MAX(OTHERSTAT_2_VALUE) MAX_STAT_2,
  MIN(OTHERSTAT_2_VALUE) MIN_STAT_2,
  AVG(OTHERSTAT_2_VALUE) AVG_STAT_2,
  MAX(OTHERSTAT_3_VALUE) MAX_STAT_3,
  MIN(OTHERSTAT_3_VALUE) MIN_STAT_3,
  AVG(OTHERSTAT_3_VALUE) AVG_STAT_3,
  MAX(OTHERSTAT_4_VALUE) MAX_STAT_4,
  MIN(OTHERSTAT_4_VALUE) MIN_STAT_4,
  AVG(OTHERSTAT_4_VALUE) AVG_STAT_4,
  MAX(OTHERSTAT_5_VALUE) MAX_STAT_5,
  MIN(OTHERSTAT_5_VALUE) MIN_STAT_5,
  AVG(OTHERSTAT_5_VALUE) AVG_STAT_5,
  MAX(OTHERSTAT_6_VALUE) MAX_STAT_6,
  MIN(OTHERSTAT_6_VALUE) MIN_STAT_6,
  AVG(OTHERSTAT_6_VALUE) AVG_STAT_6
from
  oceanbase.gv$sql_plan_monitor
where
  trace_id = '{trace_id}'
GROUP BY
  PLAN_LINE_ID,PLAN_OPERATION,SVR_IP,SVR_PORT
ORDER BY
  {order_by}
'''

sql_plan_monitor_svr_agg_template = (sql_plan_monitor_svr_agg_template if tenantMode == 'mysql' else sql_plan_monitor_svr_agg_oracle_template)
sql_plan_monitor_svr_agg_v1 = sql_plan_monitor_svr_agg_template.format(trace_id=trace_id, order_by="PLAN_LINE_ID ASC, MAX_CHANGE_TIME ASC, SVR_IP, SVR_PORT");
sql_plan_monitor_svr_agg_v2 = sql_plan_monitor_svr_agg_template.format(trace_id=trace_id, order_by="SVR_IP, SVR_PORT, PLAN_LINE_ID");

sql_plan_monitor_detail_oracle_template = '''
select
  PLAN_DEPTH,
  SVR_IP,
  SVR_PORT,
  PROCESS_NAME,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  OUTPUT_ROWS,
  STARTS RESCAN_TIMES,
  FIRST_REFRESH_TIME,
  LAST_REFRESH_TIME,
  FIRST_CHANGE_TIME,
  LAST_CHANGE_TIME,
  24 * 3600 * extract(day FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_REFRESH_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_REFRESH_TS,
  24 * 3600 * extract(day FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 3600 * extract(hour FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + 60 * extract(minute FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) + extract(second FROM LAST_REFRESH_TIME - FIRST_REFRESH_TIME) REFRESH_TS,
  24 * 3600 * extract(day FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM FIRST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) FIRST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 3600 * extract(hour FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + 60 * extract(minute FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) + extract(second FROM LAST_CHANGE_TIME - TO_DATE('1970-01-01', 'YYYY-MM-DD')) LAST_CHANGE_TS,
  24 * 3600 * extract(day FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 3600 * extract(hour FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + 60 * extract(minute FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) + extract(second FROM LAST_CHANGE_TIME - FIRST_CHANGE_TIME) CHANGE_TS,
  OTHERSTAT_1_ID,
  OTHERSTAT_1_VALUE,
  OTHERSTAT_2_ID,
  OTHERSTAT_2_VALUE,
  OTHERSTAT_3_ID,
  OTHERSTAT_3_VALUE,
  OTHERSTAT_4_ID,
  OTHERSTAT_4_VALUE,
  OTHERSTAT_5_ID,
  OTHERSTAT_5_VALUE,
  OTHERSTAT_6_ID,
  OTHERSTAT_6_VALUE
FROM
  sys.gv$sql_plan_monitor
WHERE
    trace_id = '{trace_id}'
ORDER BY
  {order_by}
'''


sql_plan_monitor_detail_template = '''
select
  PLAN_DEPTH,
  SVR_IP,
  SVR_PORT,
  PROCESS_NAME,
  PLAN_LINE_ID,
  PLAN_OPERATION,
  OUTPUT_ROWS,
  STARTS RESCAN_TIMES,
  FIRST_REFRESH_TIME,
  LAST_REFRESH_TIME,
  FIRST_CHANGE_TIME,
  LAST_CHANGE_TIME,
  UNIX_TIMESTAMP(FIRST_REFRESH_TIME) FIRST_REFRESH_TS,
  UNIX_TIMESTAMP(LAST_REFRESH_TIME) LAST_REFRESH_TS,
  UNIX_TIMESTAMP(LAST_REFRESH_TIME) - UNIX_TIMESTAMP(FIRST_REFRESH_TIME) REFRESH_TS,
  UNIX_TIMESTAMP(FIRST_CHANGE_TIME) FIRST_CHANGE_TS,
  UNIX_TIMESTAMP(LAST_CHANGE_TIME) LAST_CHANGE_TS,
  UNIX_TIMESTAMP(LAST_CHANGE_TIME) - UNIX_TIMESTAMP(FIRST_CHANGE_TIME) CHANGE_TS,
  OTHERSTAT_1_ID,
  OTHERSTAT_1_VALUE,
  OTHERSTAT_2_ID,
  OTHERSTAT_2_VALUE,
  OTHERSTAT_3_ID,
  OTHERSTAT_3_VALUE,
  OTHERSTAT_4_ID,
  OTHERSTAT_4_VALUE,
  OTHERSTAT_5_ID,
  OTHERSTAT_5_VALUE,
  OTHERSTAT_6_ID,
  OTHERSTAT_6_VALUE
from
  oceanbase.gv$sql_plan_monitor
where
    trace_id = '{trace_id}'
ORDER BY
  {order_by}
'''


sql_plan_monitor_detail_template = (sql_plan_monitor_detail_template if tenantMode == 'mysql' else sql_plan_monitor_detail_oracle_template)
sql_plan_monitor_detail_v1 = sql_plan_monitor_detail_template.format(trace_id=trace_id, order_by="PLAN_LINE_ID ASC, SVR_IP, SVR_PORT, CHANGE_TS, PROCESS_NAME ASC");
sql_plan_monitor_detail_v2 = sql_plan_monitor_detail_template.format(trace_id=trace_id, order_by="PROCESS_NAME ASC, PLAN_LINE_ID ASC, FIRST_REFRESH_TIME ASC");

try:
  sql = audit_sql
  cursor.execute(sql);
  sql_audit_result = from_db_cursor(cursor);
  sql = full_audit_sql
  cursor.execute(sql);
  full_sql_audit_result = from_db_cursor(cursor);
  # plan explain
  sql = plan_explain_sql
  cursor.execute(sql)
  plan_explain_result = from_db_cursor(cursor);
  # dop = N as it is in the sql, without session
  sql = explain_sql
  cursor.execute(sql);
  sql_explain_result = cursor.fetchone();
  # parallel it is
  sql = go_parallel_query_sql
  cursor.execute(sql);
  sql = go_parallel_dml_sql
  cursor.execute(sql);
  sql = explain_sql
  cursor.execute(sql);
  sql_explain_result_parallel = cursor.fetchone();

  sql = sql_plan_monitor_dfo_agg
  cursor.execute(sql)
  sql_plan_monitor_dfo_agg_result = from_db_cursor(cursor);
  sql = sql_plan_monitor_svr_agg_v1
  cursor.execute(sql_plan_monitor_svr_agg_v1)
  sql_plan_monitor_svr_agg_result = from_db_cursor(cursor);
  sql = sql_plan_monitor_detail_v1
  cursor.execute(sql_plan_monitor_detail_v1)
  sql_plan_monitor_detail_result = from_db_cursor(cursor);
except Exception as e:
  print("> %s" % (sql))
  print(repr(e))
  sys.exit(2)

print_header();
# sql_audit ??????
report(sql_audit_result.get_html_string())

# explain
print_pre(sql_explain_result);
print_pre(sql_explain_result_parallel);
print_pre(plan_explain_sql);
plan_explain_result.align = 'l';
print_pre(plan_explain_result);

print_schema(cursor, user_sql, enableDumpDb)
# sql audit ??????
if not enableFastDump:
	report("<div><h2 id='sql_audit_table_anchor'>SQL_AUDIT ??????</h2><div class='v' id='sql_audit_table' style='display: none'>" + full_sql_audit_result.get_html_string() + "</div></div>")

# ?????? agg ???+???
# print_pre(sql_plan_monitor_dfo_agg) # ?????? sql
report("<div><h2 id='agg_table_anchor'>SQL_PLAN_MONITOR DFO ?????????????????????</h2><div class='v' id='agg_table' style='display: none'>" + sql_plan_monitor_dfo_agg_result.get_html_string() + "</div></div>")
cursor.execute(sql_plan_monitor_dfo_agg)
print_dfo_sched_agg_graph_data(cursor, '???????????????')
cursor.execute(sql_plan_monitor_dfo_agg)
print_dfo_agg_graph_data(cursor, '???????????????')

# svr ??? agg ???+???
# print_pre(sql_plan_monitor_svr_agg) # ?????? sql
report("<div><h2 id='svr_agg_table_anchor'>SQL_PLAN_MONITOR SQC ?????????</h2><div class='v' id='svr_agg_table' style='display: none'>" + sql_plan_monitor_svr_agg_result.get_html_string() + "</div><div class='shortcut'><a href='#svr_agg_serial_v1'>Goto ????????????</a> <a href='#svr_agg_serial_v2'>Goto ????????????</a></div></div>")
cursor.execute(sql_plan_monitor_svr_agg_v1)
print_svr_agg_graph_data('svr_agg_serial_v1', cursor, '??????????????????')
cursor.execute(sql_plan_monitor_svr_agg_v2)
print_svr_agg_graph_data('svr_agg_serial_v2', cursor, '??????????????????')

print_fast_preview();

# detail ???+???
# print_pre(sql_plan_monitor_detail)  # ?????? sql
## ????????????
### ?????? op ??????
report("<div><h2 id='detail_table_anchor'>SQL_PLAN_MONITOR ??????</h2><div class='v' id='detail_table' style='display: none'>" + ("no result in --fast mode" if enableFastDump else sql_plan_monitor_detail_result.get_html_string()) + "</div><div class='shortcut'><a href='#detail_serial_v1'>Goto ????????????</a> <a href='#detail_serial_v2'>Goto ????????????</a></div></div>")
cursor.execute(sql_plan_monitor_detail_v1)
print_detail_graph_data("detail_serial_v1", cursor,'??????????????????')
### ??????????????????
cursor.execute(sql_plan_monitor_detail_v2)
print_detail_graph_data("detail_serial_v2", cursor, '??????????????????')

print_footer();

# ?????????????????????
db.close()

print("Report File Generate OK: %s" % (reportFileName))
