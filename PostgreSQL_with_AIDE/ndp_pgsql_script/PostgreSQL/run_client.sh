#!/bin/bash

IROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/

database=chbench
port=7777

./pgsql/bin/psql -d $database -p $port --username $USER
