
# Parse parameters.
for i in "$@"
do
  case $i in
    -w=*|--warenum=*)
      WARENUM="${i#*=}" 
      shift
      ;;

    -p=*|--port=*)
      SETPORT="${i#*=}" 
      shift
      ;;

    *)
      # unknown option
      ;;
  esac
done

../ndp_pgsql_script/PostgreSQL/sysbench/sysbench/build/bin/sysbench \
	--time=${PGDURATION} \
	../ndp_pgsql_script/PostgreSQL/sysbench/sysbench/build/share/sysbench/oltp2_update.lua \
	--warehouses=${PGWARENUM} \
	--db-driver=pgsql \
	--pgsql-host=127.0.0.1 --pgsql-port=${PGPORT} \
	--pgsql-user=$USER --pgsql-password=chbench \
	--pgsql-db=chbench \
	--threads=16 \
	--report_interval=1 \
run
