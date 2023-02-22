#!/bin/bash                                                                        
                                                                                   
# fail if trying to reference a variable that is not set.                          
set -u                                                                             
# exit immediately if a command fails                                              
set -e                                                                             
# echo commands                                                                    
set -x                                                                             
                                                                                   
#if [ $# -eq 4 ] ; then                                                            
#    version=$1                                                                    
#    file_name=$1                                                                  
#    is_tpcc=${2:-true}                                                            
#    is_ch=${3:-false}                                                             
#    shift                                                                         
#else                                                                              
#    version="4.0"                                                                 
#    file_name=version                                                             
#    is_tpcc=${2:-true}                                                            
#    is_ch=${3:-true}                                                              
#fi                                                                                
#file_name=$1                                                                      
#is_tpcc=${2:-true}                                                                
#is_ch=${3:-false}                                                                 
 
WARENUM="2"
SETPORT="7777"

# Parse parameters.
for i in "$@"
do
case $i in
    -w=*|--warenum=*)
    WARENUM="${i#*=}" 
    shift
    ;;

    -p=*|--port=*)
    SETPORT="${i#*=}" 
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done


VUNUM=$WARENUM
 
# Parse parameters.
for i in "$@"
do
case $i in
    -vu=*|--vunum=*)
    VUNUM="${i#*=}" 
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done

CORENUM=$(nproc --all)

if [ $((VUNUM)) -gt $((CORENUM)) ]
then
	VUNUM=$CORENUM
fi


if [ $((VUNUM)) -gt $((WARENUM)) ]
then
	VUNUM=$WARENUM
fi

version="4.4"                                                                      
file_name=version                                                                  
is_tpcc="true"                                                                     
is_ch="true"                                                                       
cur_user=$USER 

export PGHOME=$PWD"/../ndp_pgsql_script/PostgreSQL/pgsql/bin"
export PGHOST=${PGHOST:-localhost}                                                 
export PGPORT=$SETPORT
export PGUSER=$cur_user
export PGDATABASE=${PGDATABASE:-chbench}                                          
export PGPASSWORD=${PGPASSWORD:-chbench}
export PGSUPERUSER=$cur_user
export PGSUPERPASSWORD=${PGSUPERPASSWORDL-postgres}
export PGWARENUM=$WARENUM
export PGNUMVU=$VUNUM

mkdir -p results/                                                                  
                                                                                   
# drop tables if they exist since we might be running hammerdb multiple times with different configs
${PGHOME}/psql -v "ON_ERROR_STOP=1" -f sql/drop-tables.sql             
                                                                                   
## set Citus configurations                                                        
#psql -c "ALTER ROLE current_user SET citus.replication_model TO 'streaming'" 2>/dev/null || true
#psql -c "ALTER ROLE current_user SET citus.shard_count TO 40" 2>/dev/null || true
#psql -c "ALTER ROLE current_user SET citus.enable_repartition_joins to on" 2>/dev/null || true
                                                                                   
# build hammerdb related tables                                                    
test -d "HammerDB-$version" || ./generate-hammerdb.sh "$version"                   
(cd HammerDB-$version && time ./hammerdbcli auto ../build.tcl | tee "../results/hammerdb_build_${file_name}.log")
                                                                                   
#if [ $is_ch = true ] ; then                                                       
  # create ch-benchmark tables in cluster                                          
${PGHOME}/psql -v "ON_ERROR_STOP=1" -f sql/ch-benchmark-tables.sql                 
                                                                                   
  # distribute ch-benchmark tables                                                 
#  psql -f sql/ch-benchmark-distribute.sql                                         
#fi 
