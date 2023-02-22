#!/bin/bash

# Change to this-file-exist-path.
IROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $IROOTDIR

GDB=NO
NDP=NO
INCLUDE_NDP_LIBRARY=NO
INIT=NO
REMOVE_LOG=NO
NEW_DB_USER=NO
VERSION_INDEX=NO
CONFIGURE=YES
DBUF_HDEBUG=NO
ADD_PATH=YES
PROFILE=NO

INSTALL_PARAM=""

for i in "$@"
do
case $i in
	--gdb)
	GDB=YES
	shift
	;;

	--ndp)
	NDP=YES
	shift
	;;

	--no-configure)
	CONFIGURE=NO
	shift
	;;

	--init_db)
	INIT=YES
	NEW_DB_USER=YES
	REMOVE_LOG=YES
	shift
	;;

    --profile)
    PROFILE=YES
    shift
    ;;

	--include-ndp-library)
	INCLUDE_NDP_LIBRARY=YES
	shift
	;;

	--remove_log)
	REMOVE_LOG=YES
	shift
	;;

	--createdb)
	NEW_DB_USER=YES
	shift
	;;

	--with_vi)
	VERSION_INDEX=YES
	shift
	;;

	--hard-buf-debug)
	DBUF_HDEBUG=YES
	shift
	;;

	--no-kernel-path)
	ADD_PATH=NO
	shift
	;;	

	*)

	;;
esac
done

if [ "$GDB" == "NO" ]
then
	INSTALL_PARAM+=" --no-gdb"
fi

if [ "$NDP" == "NO" ]
then
	INSTALL_PARAM+=" --no-ndp"
fi

if [ "$ADD_PATH" == "NO" ]
then
	INSTALL_PARAM+=" --no-kernel-path"
fi

if [ "$PROFILE" == "YES" ]
then
	INSTALL_PARAM+=" --profile"
fi

if [ "$CONFIGURE" == "NO" ]
then
	INSTALL_PARAM+=" --no-configure"
fi

if [ "$INCLUDE_NDP_LIBRARY" == "YES" ]
then
	INSTALL_PARAM+=" --include-ndp-library"
fi

if [ "$VERSION_INDEX" == "YES" ]
then
	INSTALL_PARAM+=" --with_vi"
fi

if [ "$DBUF_HDEBUG" == "YES" ]
then
	INSTALL_PARAM+=" --hard-buf-debug"
fi

./script/script_install/install.sh $INSTALL_PARAM

if [ "$INIT" == "YES" ]
then
	cd $IROOTDIR
	./script/script_server/init_server.sh 
else
	printf "Skip DB initialization\n"
	cd $IROOTDIR
	printf "copy new config file to postgres data directory\n"
	cp postgresql.conf-std data/postgresql.conf
fi

cd $IROOTDIR
if [ "$REMOVE_LOG" == "YES" ]
then
	./script/script_server/run_server.sh --pghint --remove_log
else
	printf "do not remove logfile\n"
	./script/script_server/run_server.sh
fi

if [ "$NEW_DB_USER" == "YES" ]
then
	cd $IROOTDIR
	./script/script_client/create_db.sh
else
	printf "Skip DB create\n"
	printf "Skip USER create\n"
fi

cd $IROOTDIR
./script/script_server/shutdown_server.sh
