#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"

# Parse parameters.
for i in "$@"
do
case $i in
    -b=*|--base_dir=*)
    BASE_DIR="${i#*=}"
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

# server start
rm -rf "$BASE_DIR""data"
"$BIN_DIR""initdb" -D "$BASE_DIR""data/"
cp $BASE_DIR"postgresql.conf-std" $BASE_DIR"data/postgresql.conf"
