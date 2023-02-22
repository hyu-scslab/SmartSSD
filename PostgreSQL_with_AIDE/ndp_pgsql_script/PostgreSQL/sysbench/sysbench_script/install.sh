#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
PGPATH="../../pgsql/"
cd $DIR

# Default
BASE_DIR="$DIR""../sysbench/"

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

cd $BASE_DIR
TARGET_DIR="$BASE_DIR""/build"

make clean -j --silent
make uninstall -j 

rm -rf $TARGET_DIR

mkdir $TARGET_DIR

./autogen.sh
./configure --without-mysql --with-pgsql --silent --prefix=$TARGET_DIR --exec-prefix=$TARGET_DIR 

make -j --silent
make install -j

cd $DIR


