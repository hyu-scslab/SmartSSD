#!/bin/bash

# Parse parameters
for i in "$@"
do
  case $i in
    --src-dir=*)
      SRC_DIR="${i#*=}"
      shift
      ;;

    --inst-dir=*)
      INST_DIR="${i#*=}"
      shift
      ;;

    --data-dir=*)
      DATA_DIR="${i#*=}"
      shift
      ;;

    --compile-option=*)
      COMPILE_OPTION="${i#*=}"
      shift
      ;;

    --gdb)
      GDB=YES
      shift
      ;;

    --mysql-port=*)
      MYSQL_PORT="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      echo "Unknown option: ${i}"
      exit
      ;;
  esac
done

cd ${SRC_DIR}

make clean
rm -f CMakeCache.txt

cmake . \
  -DCMAKE_BUILD_TYPE=Release \
  -DWITH_SSL=system \
  -DWITH_ZLIB=bundled \
  -DMYSQL_MAINTAINER_MODE=0 \
  -DENABLED_LOCAL_INFILE=1 \
  -DENABLE_DTRACE=0 \
  -DCMAKE_CXX_FLAGS="-Wno-implicit-fallthrough -Wno-int-in-bool-context \
  -Wno-shift-negative-value -Wno-misleading-indentation \
  -Wno-format-overflow -Wno-nonnull -Wno-unused-function \
  -Wno-aligned-new -march=native \
  -Wno-invalid-offsetof \
  ${COMPILE_OPTION}" \
  -DWITH_ZSTD=/usr \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DCMAKE_INSTALL_PREFIX=${INST_DIR} \
  -DMYSQL_UNIX_ADDR=${INST_DIR}/mysql.sock \
  -DSYSCONFDIR=${INST_DIR} \
  -DMYSQL_DATADIR=${DATA_DIR} \
  -DINSTALL_DOCDIR=${INST_DIR} \
  -DINSTALL_INFODIR=${INST_DIR} \
  -DINSTALL_DOCREADMEDIR=${INST_DIR} \
  -DENABLE_DOWNLOADS=1 \
  -DMYSQL_TCP_PORT=${MYSQL_PORT}

make -j32
make install -j32
