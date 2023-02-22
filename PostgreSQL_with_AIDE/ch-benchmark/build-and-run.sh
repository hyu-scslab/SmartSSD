#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x

if [ $# -eq 4 ] ; then
    version=$1
    shift
else version="4.4"
fi
file_name=$1
is_tpcc=${2:-true}
is_ch=${3:-false}

export PGHOME=$PWD"/../ndp_pgsql_script/PostgreSQL/pgsql/bin"
export PGHOST=${PGHOST:-localhost}                                                 
export PGPORT=${PGPORT:-7777}                                                      
export PGUSER=${PGUSER:-chbench}                                                   
export PGDATABASE=${PGDATABASE:-chbench}                                          
export PGPASSWORD=${PGPASSWORD:-chbench}

mkdir -p results/

# drop tables if they exist since we might be running hammerdb multiple times with different configs
${PGHOME}/psql -v "ON_ERROR_STOP=1" -f sql/drop-tables.sql

## set Citus configurations
#psql -c "ALTER ROLE current_user SET citus.replication_model TO 'streaming'" 2>/dev/null || true
#psql -c "ALTER ROLE current_user SET citus.shard_count TO 40" 2>/dev/null || true
#psql -c "ALTER ROLE current_user SET citus.enable_repartition_joins to on" 2>/dev/null || true

# build hammerdb related tables
test -d "HammerDB-$version" || ./generate-hammerdb.sh "$version"
(cd HammerDB-$version && time ./hammerdbcli auto ../build.tcl | tee "../results/hammerdb_build_${file_name}.log")

if [ $is_ch = true ] ; then
  # create ch-benchmark tables in cluster
  ${PGHOME}/psql -v "ON_ERROR_STOP=1" -f sql/ch-benchmark-tables.sql

  # distribute ch-benchmark tables
#  psql -f sql/ch-benchmark-distribute.sql
fi

exec ./run.sh "$version" "$@"
