#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"
USER="sbtest"
PORT="7777"


# Parse parameters.
for i in "$@"
do
case $i in
    -b=*|--base_dir=*)
    BASE_DIR="${i#*=}"
    shift
    ;;

    -u=*|--user=*)
    USER="${i#*=}"
    shift
    ;;

    -p=*|--port=*)
    PORT="${i#*=}"
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done


TARGET_DIR=$BASE_DIR"pgsql/"
BIN_DIR=$TARGET_DIR"bin/"

LD_LIBRARY_PATH=$TARGET_DIR"lib":$LD_LIBRARY_PATH
export LD_LIBRARY_PATH



# run client
"$BIN_DIR""psql" -p "$PORT" -d sbtest -U "$USER"




