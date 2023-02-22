./sysbench/sysbench/build/bin/sysbench \
	--time=3600 \
	./sysbench/sysbench/build/share/sysbench/oltp2_update.lua \
	--warehouses=2 \
	--db-driver=pgsql \
	--pgsql-host=127.0.0.1 --pgsql-port=7777 \
	--pgsql-user=ktlee20 --pgsql-password=chbench \
	--pgsql-db=chbench \
	--threads=32 \
	--report_interval=1 \
run
