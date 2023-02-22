rm -rf ../ndp_pgsql_script/PostgreSQL/data/
cp -r ../ndp_pgsql_script/PostgreSQL/data-backup-w2/ ../ndp_pgsql_script/PostgreSQL/data/
cgexec -g memory:aide ../ndp_pgsql_script/PostgreSQL/pgsql/bin/pg_ctl -D ../ndp_pgsql_script/PostgreSQL/data/  -l ../ndp_pgsql_script/PostgreSQL/logfile start
sleep 10
./run_resource_contention_wh2_aide.sh -q=8
echo "end"
sleep 15
../ndp_pgsql_script/PostgreSQL/shutdown_server.sh
sleep 10

