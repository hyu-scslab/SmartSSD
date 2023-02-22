#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --base-dir=*)
      BASE_DIR="${i#*=}"
      shift
      ;;

    --src-dir=*)
      SRC_DIR="${i#*=}"
      shift
      ;;

    --install-dir=*)
      INSTALL_DIR="${i#*=}"
      shift
      ;;

    --compile-option=*)
      COMPILE_OPTION="${i#*=}"
      shift
      ;;

    --lib-option=*)
      LIB_OPTION="${i#*=}"
      shift
      ;;

    --gdb)
      GDB=YES
      shift
      ;;

    --extension-dir=*)
      EXTENSION_DIR="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

# GDB
if [ "${GDB}"=="YES" ]
then
  COMPILE_OPTION="${COMPILE_OPTION} -O0 -g -fno-omit-frame-pointer"
fi

# Postgres source directory
cd ${SRC_DIR}

# Cleanup
make clean -j --silent

# Configure
./configure --silent --prefix=${INSTALL_DIR} --enable-cassert --enable-debug CFLAGS="${COMPILE_OPTION}" LDFLAGS="${LIB_OPTION}"

# Build & Install PostgreSQL
make -j --silent
make install -j --silent

# Build & Install Extensions
cd ${EXTENSION_DIR}

make -j --silent
make install -j --silent

