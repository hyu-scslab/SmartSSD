#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/
cd $DIR

# Default
BASE_DIR="$DIR""../../../PostgreSQL/"
CONFIGURE=YES
NDP=YES
INCLUDE_NDP_LIBRARY=NO
GDB=YES
NEW_POST_CONFIG=YES
LIB_OPTION=""
VERSION_INDEX=NO
DBUF_HDEBUG=NO
ADD_PATH=YES
PROFILE=NO

printf "Install parameter input :\n"
echo $@

sleep 2

# Parse parameters.
for i in "$@"
do
case $i in
    -b=*|--base_dir=*)
    BASE_DIR="${i#*=}"
    shift
    ;;

    -c=*|--compile_option=*)
    COMPILE_OPTION="${i#*=}"
    shift
    ;;

    --no-configure)
    CONFIGURE=NO
    shift
    ;;

    --profile)
    PROFILE=YES
    shift
    ;;

    --no-gdb)
    GDB=NO
    shift
    ;;

		--no-ndp)
		NDP=NO
		shift
		;;

		--include-ndp-library)
		INCLUDE_NDP_LIBRARY=YES
		shift
		;;

		--use_existing_postconf)
		NEW_POST_CONFIG=NO
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
          # unknown option
    ;;
esac
done

SOURCE_DIR=$BASE_DIR"../../postgres/"
TARGET_DIR=$BASE_DIR"pgsql/"
EXTENSION_DIR=$SOURCE_DIR"contrib/"

cd $SOURCE_DIR

printf "Waiting make clean...\n"
make clean -j96 --silent

# ndp
if [ "$NDP" == "YES" ]
then
	if [ "$DBUF_HDEBUG" == "YES" ]
	then
    COMPILE_OPTION+=" -DSMARTSSD_DBUF "
	else
    COMPILE_OPTION+=" -DSMARTSSD -DSMARTSSD_DBUF "
	fi
    if [ "$VERSION_INDEX" == "YES" ]
    then
			printf "Setting for generating version index"
    	COMPILE_OPTION+=" -DVERSION_INDEX -DNUM_VI_PAGE=51200"
		fi
    if [ "$INCLUDE_NDP_LIBRARY" == "YES" ]
    then
        COMPILE_OPTION+=" -DSMARTSSD_EXISTS"
        LIB_OPTION+=" -L/opt/xilinx/xrt/lib -lndpf -lstdc++ -lxilinxopencl"
    fi
fi

# gdb
if [ "$GDB" == "YES" ]
then
    COMPILE_OPTION+=" -ggdb -O0 -g -fno-omit-frame-pointer"
    if [ "$NDP" == "YES" ]
    then
		if [ "$DBUF_HDEBUG" == "YES" ]
		then
    		COMPILE_OPTION+=" -DSMARTSSD_DBUF_DEBUG -DDBUF_HARD_DEBUG"
		else
      		COMPILE_OPTION+=" -DSMARTSSD_DEBUG -DSMARTSSD_DBUF_DEBUG"
		fi
    	if [ "$VERSION_INDEX" == "YES" ]
		then
      		COMPILE_OPTION+=" -DVERSION_INDEX_DEBUG -DWSUL_CTID_TEST"
		fi
    fi
fi

if [ "$PROFILE" == "YES" ]
then
	COMPILE_OPTION+=" -DSMARTSSD_TIME_PROFILE"
fi

printf "*-Configuration setting-*\n"
printf "CONFIGURE : %s\n" "$CONFIGURE"
printf "GDB : %s\n" "$GDB"
printf "NDP : %s\n" "$NDP"
printf "INCLUDE_NDP_LIBRARY : %s\n" "$INCLUDE_NDP_LIBRARY"
printf "MAKE NEW POST CONFIG :%s\n\n" "$NEW_POST_CONFIG"

sleep 1

printf "COMPILE_OPTION : %s\n" "$COMPILE_OPTION"
printf "LIB_OPTION : %s\n\n" "$LIB_OPTION"
sleep 3

# configure
if [ "$CONFIGURE" == "YES" ]
then
		printf "Waiting configuring...\n"
    # ./configure --silent --prefix=$TARGET_DIR CFLAGS="$COMPILE_OPTION" LDFLAGS="$LIB_OPTION"
    ./configure --silent --enable-cassert --prefix=$TARGET_DIR CFLAGS="$COMPILE_OPTION" LDFLAGS="$LIB_OPTION"
fi

printf "Building postgres..."
make -j -s
printf "..."

make install -j -s
printf "\n\n"

printf "Building postgres extension..."
cd $EXTENSION_DIR

make -j -s
printf "..."

make install -j -s
printf "\n\n"

cd $DIR
cd ../../../../
KERNEL_PATH=$PWD"/binary_container_1.xclbin"

cd $DIR
if [ "$NEW_POST_CONFIG" == "YES" ]
then
	printf "Create postgres config file\n"
	cp ../../postgresql.conf-src ../../postgresql.conf-std
	if [ "$NDP" == "YES" ] && [ "$ADD_PATH" == "YES" ]
	then
		printf "Writing kernel path to postgres config file\n"
		QUERY="\n\n#------------------------------------------------------------------------------\n"
		QUERY+="# CUSTOMIZED OPTIONS\n"
		QUERY+="#------------------------------------------------------------------------------\n\n"
		QUERY+="# Add settings for extensions here\n"
		QUERY+="smartssd_kernel_file = '""$KERNEL_PATH""'"
		echo -e $QUERY >> ../../postgresql.conf-std
	fi
fi

