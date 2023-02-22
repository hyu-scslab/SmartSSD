#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"
PORT="7777"
DBNAME="postgres"
CURUSER=$USER

# Parse parameters.
for i in "$@"
do
case $i in
    -b=*|--base_dir=*)
    BASE_DIR="${i#*=}"
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


QUERY="ALTER USER "
QUERY+=$CURUSER
QUERY+=" WITH PASSWORD 'postgres';"
QUERY+="CREATE USER sbtest;"
QUERY+="CREATE DATABASE sbtest;"
QUERY+="GRANT ALL PRIVILEGES ON DATABASE sbtest TO sbtest;"
QUERY+="CREATE USER chbench;"
QUERY+="CREATE DATABASE chbench;"
QUERY+="ALTER USER chbench WITH PASSWORD 'chbench';"
QUERY+="GRANT ALL PRIVILEGES ON DATABASE chbench TO chbench;"

# run query
echo "$QUERY" | "$BIN_DIR""psql" -p "$PORT" -d "$DBNAME"
