#!/bin/bash

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

    --error-logfile=*)
      ERROR_LOGFILE="${i#*=}"
      shift
      ;;

    --skip_stack_trace)
      SKIP_ST='--skip_stack_trace'
      shift
      ;;

    --cgroup)
      CGROUP=YES
      shift
      ;;

    --gdb)
      GDB='--gdb'
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

CGROUP_COMMAND=""
# Server start
if [ "${CGROUP}" == "YES" ]
then
  CGROUP_COMMAND+="cgexec -g memory:aide-myrocks"
fi

cd ${INST_DIR}

./bin/mysqladmin -uroot shutdown

${CGROUP_COMMAND} ./bin/mysqld --defaults-file=${CONFIG_FILE} ${SKIP_ST} ${GDB} &

( tail -f -n0 ${ERROR_LOGFILE} & ) | grep -q "ready for connections"
#sleep 10

./bin/mysql --defaults-file=${CONFIG_FILE} -u root -e "CREATE DATABASE IF NOT EXISTS sbtest;"

