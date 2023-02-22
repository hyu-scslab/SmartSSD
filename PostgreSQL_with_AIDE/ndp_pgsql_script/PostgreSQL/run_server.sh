#!/bin/bash

# Change to this-file-exist-path.
IROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/ 
cd $IROOTDIR

BUILD_CH=NO
WARENUM=100 #100

INSTALL_PARAM=""

for i in "$@"
do
case $i in
  --buildch)
  BUILD_CH=YES
  shift
  ;;

  -w=*|--warenum=*)
  WARENUM="${i#*=}" 
  shift
  ;;

  *)

  ;;
esac
done

PARWN="-w=$WARENUM"

cd $IROOTDIR
./script/script_server/run_server.sh

if [ "$BUILD_CH" == "YES" ]
then
  printf "\nBUILD CHTABLES\n"
  sleep 2
  cd $IROOTDIR
  ./script/script_chbench/build_tables.sh $PARWN
fi

