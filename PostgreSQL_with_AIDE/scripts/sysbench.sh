#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --src-dir=*)
      SRC_DIR="${i#*=}"
      shift
      ;;

    --pgsql-user=*)
      USER="${i#*=}"
      shift
      ;;

    --pgsql-host=*)
      HOST="${i#*=}"
      shift
      ;;

    --pgsql-port=*)
      PORT="${i#*=}"
      shift
      ;;

    --pgsql-db=*)
      DB="${i#*=}"
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

    --time-oltp-only=*)
      TIME_OLTP_ONLY="${i#*=}"
      shift
      ;;

    --time-olap-only=*)
      TIME_OLAP_ONLY="${i#*=}"
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

cd ${SRC_DIR}
cd ../

./src/sysbench \
    --pgsql-user=${USER} \
    --pgsql-host=${HOST} \
    --pgsql-port=${PORT} \
    --pgsql-db=${DB} \
    --table-size=${TABLE_SIZE} \
    --tables=${TABLES} \
    --time=`expr ${TIME} - ${TIME_OLAP_ONLY}` \
    --threads=${THREADS} \
    --report-interval=${REPORT_INTERVAL} \
    --secondary=${SECONDARY} \
    --create-secondary=${CREATE_SECONDARY} \
    --warmup-time=${WARMUP_TIME} \
    --rand-type=${RAND_TYPE} \
    --rand-zipfian-exp=${RAND_ZIPFIAN_EXP} \
    ${LUA} ${MODE}

