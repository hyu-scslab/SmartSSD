#!/bin/bash

cd inst
./bin/mysql --defaults-file=../config/my.cnf -u root -e "SHOW ENGINE ROCKSDB STATUS;"
