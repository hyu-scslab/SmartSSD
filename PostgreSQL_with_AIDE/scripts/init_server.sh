#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --bin-dir=*)
      BIN_DIR="${i#*=}"
      shift
      ;;

    --lib-dir=*)
      LIB_DIR="${i#*=}"
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

    *)
      # unknown option
      ;;
  esac
done

# Init server (create data directory, etc.)
rm -rf ${DATA_DIR}
${BIN_DIR}/initdb -D ${DATA_DIR}

cp ${CONFIG_FILE} ${DATA_DIR}

