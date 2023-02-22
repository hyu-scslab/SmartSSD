#!/bin/bash

# Usage:
# ./run.sh [--hammerdb-version[=]<version>] [--ch|--ch-queries-only] [--no-citus] [--name[=]name] [--shard-count[=]<shard_count>]

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x
# fail if a command that is piped fails
set -o pipefail

CUR_DIR="$( cd "$( dirname "$0" )" && pwd )" # myrocks dir
# MYSQL_CLIENT="$CUR_DIR/../inst/bin/mysql"
MYSQL_CLIENT="$CUR_DIR/../inst/bin/mysql"

CH_THREAD_COUNT=${CH_THREAD_COUNT:-1}
RAMPUP_TIME=${RAMPUP_TIME:-1}
DEFAULT_CH_RUNTIME_IN_SECS=${DEFAULT_CH_RUNTIME_IN_SECS:-120}

source parse-arguments.sh
mkdir -p results/

if [ $TARGET_DB = pg ]; then
psql -P pager=off -f sql/vacuum-ch.sql
psql -P pager=off -f sql/vacuum-tpcc.sql
psql -P pager=off -f sql/do-checkpoint.sql
fi

IS_CH=false
if [ $TARGET_DB = my ]; then
    if [ "$IS_CH" = true ] ; then
        ./ch_benchmark.py "${CH_THREAD_COUNT}" "${DBHOST}" "${RAMPUP_TIME}" "${BENCHNAME}" >> results/"ch_benchmarks_${BENCHNAME}.log" &
        ch_pid=$!
        echo ${ch_pid}
    fi
    if [ "$IS_TPCC" = true ] ; then
        # run hammerdb tpcc benchmark
        ./download-hammerdb.sh "$HAMMERDB_VERSION"
        (cd "HammerDB-$HAMMERDB_VERSION" && time ./hammerdbcli auto ../my_run.tcl | tee "../results/hammerdb_run_${BENCHNAME}.log")
        # filter and save the NOPM (new orders per minute) to a new file
        grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${BENCHNAME}.log" | tee -a "./results/hammerdb_nopm_${BENCHNAME}.log"
    elif [ "$IS_CH" = true ] ; then
        sleep "$DEFAULT_CH_RUNTIME_IN_SECS"
    fi
    if [ "$IS_CH" = true ] ; then
        kill ${ch_pid}
        sleep 30
    fi
else
    if [ "$IS_CH" = true ] ; then
        ./ch_benchmark_resource_contention_aide_perq_plot.py 0 "${CH_THREAD_COUNT}" "${RAMPUP_TIME}" false 0 "${BENCHNAME}" >> results/"ch_benchmarks_${BENCHNAME}.log" &
        ch_pid=$!
        echo ${ch_pid}
    fi
    if [ "$IS_TPCC" = true ] ; then
        # run hammerdb tpcc benchmark
        ./download-hammerdb.sh "$HAMMERDB_VERSION"
        (cd "HammerDB-$HAMMERDB_VERSION" && time ./hammerdbcli auto ../run.tcl | tee "../results/hammerdb_run_${BENCHNAME}.log")
        # filter and save the NOPM (new orders per minute) to a new file
        grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${BENCHNAME}.log" | tee -a "./results/hammerdb_nopm_${BENCHNAME}.log"
    elif [ "$IS_CH" = true ] ; then
        sleep "$DEFAULT_CH_RUNTIME_IN_SECS"
    fi
    if [ "$IS_CH" = true ] ; then
        kill ${ch_pid}
        sleep 30
    fi
fi
