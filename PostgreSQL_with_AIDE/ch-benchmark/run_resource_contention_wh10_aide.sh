#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x


OLAP_RAMPUP_TIME=0

#version=$1
#file_name=$2
#is_tpcc=$3
#is_ch=$4

version="4.4"
file_name=version
is_tpcc="true"
is_ch="true"
cur_user=$USER
AIDE="true"
 
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

VUNUM=24

LONG_CH_THREAD_COUNT=22
SHORT_CH_THREAD_COUNT=22

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

QUERY_NUM=-1

# Parse parameters.
for i in "$@"
do
  case $i in
    -nc=*|--num_c=*)
      VUNUM="${i#*=}" 
      shift
      ;;

    -nsh=*|--num_shorth=*)
      SHORT_CH_THREAD_COUNT="${i#*=}" 
      shift
      ;;

    -nlh=*|--num_longh=*)
      LONG_CH_THREAD_COUNT="${i#*=}"
      shift
      ;;

    -oct=*|--only_c_time=*)
      OLAP_RAMPUP_TIME="${i#*=}"
      shift
      ;;

    -q=*|--query=*)
      QUERY_NUM="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

if (( $QUERY_NUM > -1 )) ; then
  mkdir results_${QUERY_NUM}
fi

if [ $is_ch = true ] ; then
  python3 ./ch_benchmark_resource_contention_aide_perq.py ${LONG_CH_THREAD_COUNT} ${SHORT_CH_THREAD_COUNT} ${PGHOST} ${OLAP_RAMPUP_TIME} ${AIDE} ${QUERY_NUM} ${file_name} >> results_${QUERY_NUM}/ch_benchmarks_${file_name}.log &
  ch_pid=$!
  echo ${ch_pid}
fi

if [ $is_tpcc = true ] ; then
  # run hammerdb tpcc benchmark
  python3 ./run_update_stocks_w10.sh >> results_${QUERY_NUM}/run_update_stocks_${file_name}.log &
  (cd HammerDB-$version && time ./hammerdbcli auto ../run_ch_resource_content.tcl | tee "../results/hammerdb_run_${file_name}.log")
  # filter and save the NOPM (new orders per minute) to a new file
  grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${file_name}.log" >> "./results/hammerdb_nopm_${file_name}.log"

elif [ $is_ch = true ] ; then
  #sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-7200}
  sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-900}
fi

if [ $is_ch = true ] ; then
  kill ${ch_pid}
  wait
  sleep 30
fi
