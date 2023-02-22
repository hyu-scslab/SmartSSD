#!/bin/bash

BASE_DIR="$( cd "$( dirname "$0" )" && pwd )" # myrocks dir
BASE_DIR=$(pwd)
BUILD_DIR="${BASE_DIR}/build"
INST_DIR="${BASE_DIR}/inst"
DATA_DIR="${BASE_DIR}/inst/data"
SRC_DIR="${BASE_DIR}/myrocks"

echo  ${DATA_DIR}/muticore-128.pid

if [ -f "${DATA_DIR}/muticore-128.pid"]
do 
	kill -9 ${DATA_DIR}/muticore-128.pid
done

cd ${INST_DIR}

#./bin/mysqld --defaults-file=../config/my.cnf --skip-stack-trace --gdb
echo ./bin/mysqld --defaults-file=${MYCNF} ${SKIP_ST} ${GDB} 

./bin/mysqld --defaults-file=../config/my.cnf --skip-stack-trace --gdb &

while [ ! -f "${DATA_DIR}/muticore-128.pid"]
do 
	echo ${DATA_DIR}/muticore-128.pid
	cat ${DATA_DIR}/muticore-128.pid
	sleep 1
done

./bin/mysql --defaults-file=../config/my.cnf -u root -e "CREATE DATABASE IF NOT EXISTS sbtest;"

