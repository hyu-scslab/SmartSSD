#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --bin-dir=*)
      BIN_DIR="${i#*=}"
      shift
      ;;

    --port=*)
      PORT="${i#*=}"
      shift
      ;;

    --database=*)
      DATABASE="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

QUERY+="LOAD 'pg_hint_plan';"
QUERY+="CREATE EXTENSION pg_hint_plan;"
echo ${QUERY} | ${BIN_DIR}/psql -p ${PORT} -d ${DATABASE}
