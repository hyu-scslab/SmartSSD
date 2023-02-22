#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x


OLAP_RAMPUP_TIME=0

version="4.4"
file_name=version
is_tpcc="true"
is_ch="true"
is_check_file_size="false"
cur_user=$USER
AIDE="false"
GROUP="false"
FLOT="false"
 
WARENUM="10"
SETPORT="7777"
VUNUM=12

LONG_CH_THREAD_COUNT=22
SHORT_CH_THREAD_COUNT=22
QUERY_NUM=-1
OUTPUT_DIR=results_none
DURATION=900

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

    -q=*|--query=*|-g=*)
      QUERY_NUM="${i#*=}"
      shift
      ;;

	-t=*|--time=*)
	  DURATION="${i#*=}" 
	  shift
	  ;;

	--group)
	GROUP="true"
	shift
	;;

	--plot)
	PLOT="true"
	shift
	;;

	--check_file_size)
	is_check_file_size="true"
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
export PGDURATION=$DURATION
export PGDURATIONMIN=$(expr $((DURATION)) / 60 )

if (( $QUERY_NUM > -1 )) ; then
  AIDE="true"
  if (( $QUERY_NUM > 0 )) ; then
	if [ $GROUP == "true" ] ; then
    	OUTPUT_DIR=results_g${QUERY_NUM}
	else
    	OUTPUT_DIR=results_q${QUERY_NUM}
	fi
  elif (( $QUERY_NUM == 0 )) ; then
    OUTPUT_DIR=results_qall
	GROUP="true"
  fi
fi

if test -d "$OUTPUT_DIR" ; then
  rm -rf $OUTPUT_DIR
fi

mkdir $OUTPUT_DIR

${PGHOME}/psql -f sql/vacuum-ch.sql
${PGHOME}/psql -f sql/vacuum-tpcc.sql

if [ $is_ch = true ] ; then
  python3 ./ch_benchmark_resource_contention_aide_perq_plot.py ${LONG_CH_THREAD_COUNT} ${SHORT_CH_THREAD_COUNT} ${PGHOST} ${OLAP_RAMPUP_TIME} ${AIDE} ${GROUP} ${PLOT} ${QUERY_NUM} ${OUTPUT_DIR} ${file_name} >> ${OUTPUT_DIR}/ch_benchmarks_${file_name}.log &
  ch_pid=$!
  echo ${ch_pid}
fi

if [ $is_tpcc = true ] ; then
	if [ $is_check_file_size = true ] ; then
		python3 ./get_size.py ${QUERY_NUM} ${AIDE} ${OUTPUT_DIR} &
		size_pid=$!
		echo ${size_pid}
	fi

  # run hammerdb tpcc benchmark
  ./run_update_stocks.sh >> ${OUTPUT_DIR}/run_update_stocks_${file_name}.log &
  sys_pid=$!
  echo ${sys_pid}

  (cd HammerDB-$version && time ./hammerdbcli auto ../run_ch_resource_content.tcl | tee "../${OUTPUT_DIR}/hammerdb_run_${file_name}.log")
  # filter and save the NOPM (new orders per minute) to a new file
  grep -oP '[0-9]+(?= NOPM)' "./${OUTPUT_DIR}/hammerdb_run_${file_name}.log" >> "./${OUTPUT_DIR}/hammerdb_nopm_${file_name}.log"

elif [ $is_ch = true ] ; then
  sleep ${PGDURATION}
fi

if [ $is_ch = true ] ; then
  kill ${ch_pid}
  wait
fi