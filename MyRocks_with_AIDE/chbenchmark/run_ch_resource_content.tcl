#!/bin/tclsh
puts "SETTING CONFIGURATION"
global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} { after 15000 wait_to_complete } else { exit }
}
dbset db mysql
loadscript
diset connection mysql_host $env(DBHOST)
diset connection mysql_port $env(DBPORT)
diset connection mysql_socket $env(DBSOCKET)

diset tpcc mysql_dbase $env(DBDATABASE)
diset tpcc mysql_user $env(DBUSER)
diset tpcc mysql_pass $env(DBPASSWORD)
diset tpcc mysql_allwarehouse true
diset tpcc mysql_driver timed
diset tpcc mysql_rampup 0
diset tpcc mysql_timeprofile true
diset tpcc mysql_raiseerror false
diset tpcc mysql_storage_engine rocksdb

diset tpcc mysql_count_ware $env(DBWARENUM)
diset tpcc mysql_driver timed
diset tpcc mysql_duration $env(DBDURATIONMIN)

loadscript
print dict
vuset vu $env(DBVUNUM)
vuset timestamps 1
vuset logtotemp 1
vuset showoutput 0
vuset unique 1
vuset delay 100
vuset repeat 1
vurun
wait_to_complete
vwait forever
