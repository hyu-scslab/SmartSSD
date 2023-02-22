#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --bin-dir=*)
      BIN_DIR="${i#*=}"
      shift
      ;;

    --user=*)
      USER="${i#*=}"
      shift
      ;;

    --host=*)
      HOST="${i#*=}"
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

# Create User & Database
${BIN_DIR}/createuser ${USER} -d -h ${HOST} -p ${PORT} -U $(whoami) -w
${BIN_DIR}/createdb -O ${USER} -h ${HOST} -p ${PORT} -U ${USER} -w ${DATABASE}
