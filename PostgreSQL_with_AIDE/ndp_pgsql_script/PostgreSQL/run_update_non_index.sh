/opt/s3d/ktlee20/postgres-smartssd/ndp_pgsql_script/PostgreSQL/sysbench/sysbench/build/bin/sysbench \
  --time=300 \
  /opt/s3d/ktlee20/postgres-smartssd/ndp_pgsql_script/PostgreSQL/sysbench/sysbench/build/share/sysbench/oltp_update_non_index.lua \
  --table_size=100000 --tables=12 --report-interval=1 \
  --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=7777 \
  --pgsql-user=sbtest --pgsql-password=sbtest \
  --pgsql-db=sbtest \
  --threads=32  --create_secondary=false \
 run 
