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

    *)
      # unknown option
      ;;
  esac
done

# Server stop
${BIN_DIR}/pg_ctl -D ${DATA_DIR} stop

