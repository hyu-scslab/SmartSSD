#!/bin/bash

# Change to this-file-exist-path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

CHBENCH_DIR="$DIR""../../../../ch-benchmark"
cd $CHBENCH_DIR

WARENUM=2

# Parse parameters.
for i in "$@"
do
case $i in
    -w=*|--warenum=*)
    WARENUM="${i#*=}" 
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done

PARWN="-w=$WARENUM"

./build.sh  $PARWN

cd $DIR
