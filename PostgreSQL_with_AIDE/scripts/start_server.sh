#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --bin-dir=*)
      BIN_DIR="${i#*=}"
      shift
      ;;

    --data-dir=*)
      DATA_DIR="${i#*=}"
      shift
      ;;

    --logfile=*)
      LOGFILE="${i#*=}"
      shift
      ;;

    --cgroup)
      CGROUP=YES
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

rm ${LOGFILE}

CGROUP_COMMAND=""
# Server start
if [ "${CGROUP}" == "YES" ]
then
  CGROUP_COMMAND+="cgexec -g memory:aide"
fi

${CGROUP_COMMAND} ${BIN_DIR}/pg_ctl -D ${DATA_DIR} -l ${LOGFILE} start

