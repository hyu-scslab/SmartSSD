#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --mysql-socket=*)
      MYSQL_SOCKET="${i#*=}"
      shift
      ;;

    --sysbench-dir=*)
      SYSBENCH_DIR="${i#*=}"
      shift
      ;;

    --mysql-user=*)
      USER="${i#*=}"
      shift
      ;;

    --mysql-port=*)
      PORT="${i#*=}"
      shift
      ;;

    --mysql-db=*)
      DB="${i#*=}"
      shift
      ;;

    --mysql-host=*)
      HOST="${i#*=}"
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

    --report-interval=*)
      REPORT_INTERVAL="${i#*=}"
      shift
      ;;

    --secondary=*)
      SECONDARY="${i#*=}"
      shift
      ;;

    --create-secondary=*)
      CREATE_SECONDARY="${i#*=}"
      shift
      ;;

    --warmup-time=*)
      WARMUP_TIME="${i#*=}"
      shift
      ;;

    --rand-type=*)
      RAND_TYPE="${i#*=}"
      shift
      ;;

    --rand-zipfian-exp=*)
      RAND_ZIPFIAN_EXP="${i#*=}"
      shift
      ;;

    --lua=*)
      LUA="${i#*=}"
      shift
      ;;

    --mode=*)
      MODE="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

cd ${SYSBENCH_DIR}

./src/sysbench \
  --db-driver=mysql \
  --mysql-host=${HOST} \
  --mysql-port=${PORT} \
  --mysql-socket=${MYSQL_SOCKET} \
  --mysql-user=${USER} \
  --mysql-db=${DB} \
  --time=${TIME} \
  --threads=${THREADS} \
  --tables=${TABLES} \
  --table-size=${TABLE_SIZE} \
  --report-interval=${REPORT_INTERVAL} \
  --secondary=${SECONDARY} \
  --create-secondary=${CREATE_SECONDARY} \
  --warmup-time=${WARMUP_TIME} \
  --rand-type=${RAND_TYPE} \
  --rand-zipfian-exp=${RAND_ZIPFIAN_EXP} \
  --mysql-storage-engine=RocksDB \
  --auto-inc=true \
  ${LUA} ${MODE}

