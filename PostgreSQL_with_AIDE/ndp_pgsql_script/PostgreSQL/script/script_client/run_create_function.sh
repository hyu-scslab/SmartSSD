#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"
USER="sbtest"
QUERY=""
OUTPUT=""
PORT="7777"

MORE_OPTION=""

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

    -q=*|--query=*)
    QUERY="${i#*=}"
    shift
    ;;

    -p=*|--port=*)
    PORT="${i#*=}"
    shift
    ;;

    -o=*|--output=*)
    OUTPUT="${i#*=}"
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done

if [ "$QUERY" == "" ]
then
    echo "no query"
    exit
fi

if [ "$OUTPUT" != "" ]
then
    MORE_OPTION+=" ${MORE_OPTION} -o $OUTPUT "
fi


TARGET_DIR=$BASE_DIR"pgsql/"
BIN_DIR=$TARGET_DIR"bin/"

LD_LIBRARY_PATH=$TARGET_DIR"lib":$LD_LIBRARY_PATH
export LD_LIBRARY_PATH



# run query
echo "$QUERY" | "$BIN_DIR""psql" -p "$PORT" -d sbtest -U "$USER" $MORE_OPTION




