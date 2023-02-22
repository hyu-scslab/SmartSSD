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
is_tpcc="false"                                                                     
is_ch="true"                                                                       
cur_user=$USER 
 
WARENUM="2"
QUERY="0"
NDP=NO

# Parse parameters.
for i in "$@"
do
case $i in
    -w=*|--warenum=*)
    WARENUM="${i#*=}" 
    shift
    ;;

		-q=*|--query=*)
    QUERY="${i#*=}" 
		shift
		;;

		--ndp)
		NDP=YES
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

${PGHOME}/psql -f sql/vacuum-ch.sql
${PGHOME}/psql -f sql/vacuum-tpcc.sql

if [ $QUERY = "0" ] ; then
	exit 7
fi

if [ $is_ch = true ] ; then
    ./ch_benchmark_query.py ${RAMPUP_TIME} ${file_name} ${QUERY} ${NDP} >> results/ch_benchmarks_${file_name}.log 
    # ./ch_benchmark_query.py ${RAMPUP_TIME} ${file_name} ${QUERY} >> results/ch_benchmarks_${file_name}.log &
    # ch_pid=$!
    # echo ${ch_pid}
fi

if [ $is_tpcc = true ] ; then
    # run hammerdb tpcc benchmark
    (cd HammerDB-$version && time ./hammerdbcli auto ../run.tcl | tee "../results/hammerdb_run_${file_name}.log")
    # filter and save the NOPM (new orders per minute) to a new file
    grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${file_name}.log" >> "./results/hammerdb_nopm_${file_name}.log"
elif [ $is_ch = true ] ; then
    #sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-7200}
		printf "skip sleep"
fi

if [ $is_ch = true ] ; then
    #kill ${ch_pid}
    sleep 10
fi
