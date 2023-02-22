
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

./sysbench/build/bin/sysbench \
	--time=${DBDURATION} \
	./sysbench/build/share/sysbench/oltp2_update.lua \
	--warehouses=${DBWARENUM} \
  --mysql-host=localhost \
	--db-driver=mysql \
	--mysql-host=127.0.0.1 --mysql-port=${DBPORT} \
	--mysql-user=${DBUSER} --mysql-password=daktopia \
	--mysql-db=chbench \
  --mysql_storage_engine=rocksdb \
  --mysql-socket=${DBSOCKET} \
	--threads=4 \
	--report_interval=1 \
run
