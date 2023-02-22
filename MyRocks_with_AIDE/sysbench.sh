#!/bin/bash

BASEDIR="$( cd "$( dirname "$0" )" && pwd )"
USER="root"
PORT=5256
TABLE_SIZE=100000
TABLES=2
TIME=3600
THREADS=48
WORKLOAD="./sysbench/src/lua/oltp_update_non_index.lua"

# Parse parameters
for i in "$@"
do
  case $i in
    --mysql-user=*)
      USER="${i#*=}"
      shift
      ;;

    --mysql-port=*)
      PORT="${i#*=}"
      shift
      ;;

    --table-size=*)
      TABLE_SIZE="${i#*=}"
      shift
      ;;

    --tables=*)
      TABLES="${i#*=}"
      shift
      ;;

    --time=*)
      TIME="${i#*=}"
      shift
      ;;

    --threads=*)
      THREADS="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

./sysbench/src/sysbench \
  --db-driver=mysql \
  --mysql-host=localhost \
  --mysql-port=${PORT} \
  --mysql-socket="${BASEDIR}/inst/mysql.sock" \
  --mysql-user=${USER} \
  --report-interval=1 \
  --secondary=off \
  --create-secondary=false \
  --time=${TIME} \
  --threads=${THREADS} \
  --tables=${TABLES} \
  --table-size=${TABLE_SIZE} \
  --warmup-time=0 \
  --rand-type=zipfian \
  --rand-zipfian-exp=0.0 \
  --mysql-storage-engine=RocksDB \
  --auto-inc=true \
  ${WORKLOAD} cleanup

./sysbench/src/sysbench \
  --db-driver=mysql \
  --mysql-host=localhost \
  --mysql-port=${PORT} \
  --mysql-socket="${BASEDIR}/inst/mysql.sock" \
  --mysql-user=${USER} \
  --report-interval=1 \
  --secondary=off \
  --create-secondary=false \
  --time=${TIME} \
  --threads=${THREADS} \
  --tables=${TABLES} \
  --table-size=${TABLE_SIZE} \
  --warmup-time=0 \
  --rand-type=zipfian \
  --rand-zipfian-exp=0.0 \
  --mysql-storage-engine=RocksDB \
  --auto-inc=true \
  ${WORKLOAD} prepare
