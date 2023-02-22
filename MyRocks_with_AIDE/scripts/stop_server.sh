#!/bin/bash

for i in "$@"
do
  case $i in
    --inst-dir=*)
      INST_DIR="${i#*=}"
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

cd ${INST_DIR}

./bin/mysqladmin -uroot shutdown
