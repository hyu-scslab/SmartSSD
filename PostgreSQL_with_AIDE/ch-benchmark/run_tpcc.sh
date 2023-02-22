#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x

CH_THREAD_COUNT=1
RAMPUP_TIME=0

#version=$1
#file_name=$2
#is_tpcc=$3
#is_ch=$4
                                                                                   
version="4.4"
file_name=version                                                                  
is_tpcc="true"                                                                     
is_ch="false"                                                                       
cur_user=$USER 
 
WARENUM="10"
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

CORENUM=$(nproc --all)

if [ $((VUNUM)) -gt $((CORENUM)) ]
then
	VUNUM=$CORENUM
fi


if [ $((VUNUM)) -gt $((WARENUM)) ]
then
	VUNUM=$WARENUM
fi
 
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

export PGHOME=$PWD"/../ndp_pgsql_script/PostgreSQL/pgsql/bin"
export PGHOST=${PGHOST:-localhost}                                                 
export PGPORT=${PGPORT:-7777}                                                      
export PGUSER=$cur_user
export PGDATABASE=${PGDATABASE:-chbench}                                          
export PGPASSWORD=${PGPASSWORD:-chbench}
export PGSUPERUSER=$cur_user
export PGSUPERPASSWORD=${PGSUPERPASSWORDL-postgres}
export PGWARENUM=$WARENUM
export PGVUNUM=$VUNUM

${PGHOME}/psql -f sql/vacuum-ch.sql
${PGHOME}/psql -f sql/vacuum-tpcc.sql

if [ $is_ch = true ] ; then
    ./ch_benchmark.py ${CH_THREAD_COUNT} ${PGHOST} ${RAMPUP_TIME} ${file_name} >> results/ch_benchmarks_${file_name}.log &
    ch_pid=$!
    echo ${ch_pid}
fi

if [ $is_tpcc = true ] ; then
    # run hammerdb tpcc benchmark
    (cd HammerDB-$version && time ./hammerdbcli auto ../run_tpcc.tcl | tee "../results/hammerdb_run_${file_name}.log")
    # filter and save the NOPM (new orders per minute) to a new file
    grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${file_name}.log" >> "./results/hammerdb_nopm_${file_name}.log"
elif [ $is_ch = true ] ; then
    #sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-7200}
    sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-1800}
fi

if [ $is_ch = true ] ; then
    kill ${ch_pid}
    sleep 30
fi
