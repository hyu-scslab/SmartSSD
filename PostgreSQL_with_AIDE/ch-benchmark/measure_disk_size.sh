#!/bin/bash

# TPCC_FILE_PATH=/YOURPOSTGRESBASEPATH/data/base/16384
DURATION=120

for ((i=0; i<=$DURATION; i++))
do
	timestamp=$(date +%s)
	echo "@@" $timestamp >> disk_size.dat

	du -s $TPCC_FILE_PATH/* >> disk_size.dat
	sleep 1
done
