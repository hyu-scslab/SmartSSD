#!/bin/bash

BASE_DIR="$( cd "$( dirname "$0" )" && pwd )" # myrocks dir
BASE_DIR=$(pwd)
BUILD_DIR="${BASE_DIR}/build"
INST_DIR="${BASE_DIR}/inst"
DATA_DIR="${BASE_DIR}/inst/data"
SRC_DIR="${BASE_DIR}/myrocks"

# Parse parameters
for i in "$@"
do
  case $i in
    --inst-dir=*)
      INST_DIR="${i#*=}"
      shift
      ;;

    --data-dir=*)
      DATA_DIR="${i#*=}"
      shift
      ;;

    --config-file=*)
      CONFIG_FILE="${i#*=}"
      shift
      ;;

    --pid-file=*)
      PID_FILE="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      echo "Unknown option: ${i}"
      exit
      ;;
  esac
done

cd ${INST_DIR}

./bin/mysqladmin -uroot shutdown

rm -rf ${DATA_DIR}

./scripts/mysql_install_db \
		--defaults-file=${CONFIG_FILE} \
		--user=root \
		-datadir=${DATA_DIR}
