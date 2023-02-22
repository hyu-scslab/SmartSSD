#!/bin/bash

# Change to this-file-exist-path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

CHBENCH_DIR="$DIR""../../../../ch-benchmark"
cd $CHBENCH_DIR

./run.sh

cd $DIR
