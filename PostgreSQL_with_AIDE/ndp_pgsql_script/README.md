# PostgreSQL-vWeaver sysbench-base Benchmark

### Execution
```
python3 micro-benchmark.py
```

### Help
```
python3 micro-benchmark.py --help

usage: micro-benchmark.py [-h] [--run_type RUN_TYPE]
                          [--compile_option COMPILE_OPTION]
                          [--num_long_olap NUM_LONG_OLAP]
                          [--olap_wait_time OLAP_WAIT_TIME]
                          [--db-driver DB_DRIVER] [--pgsql-host PGSQL_HOST]
                          [--pgsql-db PGSQL_DB] [--pgsql-user PGSQL_USER]
                          [--pgsql-port PGSQL_PORT]
                          [--report-interval REPORT_INTERVAL]
                          [--secondary SECONDARY]
                          [--create-secondary CREATE_SECONDARY] [--time TIME]
                          [--threads THREADS] [--tables TABLES]
                          [--table-size TABLE_SIZE]
                          [--warmup-time WARMUP_TIME] [--rand-type RAND_TYPE]
                          [--rand-zipfian-exp RAND_ZIPFIAN_EXP]
                          [--workload WORKLOAD]


```
### Workload
```
The workloads are divided into multi-table join operations and update operations.
The update tarnsactions perform the oltp update non index workload of sysbench and 
the the transactions performing multi-table join operations perform joins on the table where such updates occur.
In order to make multi-table joins in a state where the version is significantly created, 
multi-table join operations are performed with a certain latency after starting transactions.
```
