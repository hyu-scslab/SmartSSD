#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"
LOGFILE="$BASE_DIR""logfile"
PGHINT=NO

PORT="7777"
DBNAME="postgres"
REMOVE_LOG=NO

# Parse parameters.
for i in "$@"
do
case $i in
    -b=*|--base_dir=*)
    BASE_DIR="${i#*=}"
    shift
    ;;

    -l=*|--logfile=*)
    LOGFILE="${i#*=}"
    shift
    ;;
		
		--pghint)
		PGHINT=YES
		shift
		;;

		--remove_log)
		REMOVE_LOG=YES
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

if [ "$REMOVE_LOG" == "YES" ]
then
rm $LOGFILE
fi

# server start
$BIN_DIR"pg_ctl" -D $BASE_DIR"data" -l $LOGFILE start

QUERY="LOAD 'pg_hint_plan';"
QUERY+="CREATE EXTENSION pg_hint_plan;"

if [ "$PGHINT" == "YES" ]
then
echo "$QUERY" | "$BIN_DIR""psql" -p "$PORT" -d "$DBNAME"
fi


