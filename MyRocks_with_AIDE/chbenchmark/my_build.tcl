#!/bin/tclsh
puts "SETTING CONFIGURATION"
global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} { after 5000 wait_to_complete } else { exit }
}
dbset db mysql
loadscript
diset connection mysql_host $env(DBHOST)
diset connection mysql_port $env(DBPORT)
diset connection mysql_socket $env(DBSOCKET)
diset tpcc mysql_dbase $env(DBDATABASE)
diset tpcc mysql_user $env(DBUSER)
diset tpcc mysql_pass $env(DBPASSWORD)
diset tpcc mysql_raiseerror true
diset tpcc mysql_num_vu 1
diset tpcc mysql_count_ware 1
diset tpcc mysql_partition false
diset tpcc mysql_storage_engine rocksdb
loadscript
print dict
buildschema
wait_to_complete
vwait forever
