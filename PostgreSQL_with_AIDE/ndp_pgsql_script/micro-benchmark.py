# pip install psycopg2
import psycopg2

import sys, subprocess
import time, threading
import os, random
import argparse
import pathlib, datetime
import json
import shutil

# 0 for MySQL, 1 for PostgreSQL
RUN_MODE=1
DB_TESTBED=0
# Prevent compile
SYSBENCH_COMP=False

DBLIST=['pgsql']
BENCHLIST=['sysbench', 'chbench']
BUILDMODE=['debug1', 'debug0', 'release']
RUNSRC=['vanilla', 'ndp', 'ndp_non_smartssd']
DB_OP_INFO=['build', 'init', 'create', 'run', 'shutdown', 'purge']
BENCH_OP_INFO={'sysbench':['init', 'run'], 'chbench':['init', 'run_psql', 'run_citus']}

# Non sysbench options
#NON_SYSBENCH=["run_mode", "compile_option", "num_short_olap", "num_long_olap", "olap_wait_time",]
DB_ARGS=["pgsql_superuser", "pgsql_port", "pgsql_superpass", "pgsql_host"]
SYSBENCH_ARGS=["pgsql_db", "pgsql_user", "pgsql_password", "db_driver", "report_interval", "secondary", "create_secondary", "time", "threads", "tables", "table_size", "warmup_time", "rand_type", "rand_zipfian_exp", "workload", "pgsql_port", "pgsql_host"] 
CHBENCH_ARGS=["warehouse", "runclient", "ch_istpcc", "ch_isch", "ch_db", "ch_user", "ch_pass", "ch_runtime", "ch_rampuptime","ch_thread_cnt", "ch_run_vu"]

MODE_ARGS=dict()

DBS=[]
DB_OP_ARGS=dict()
DB_OP_KV=dict()
DB_SETTING_ARGS=dict()
DB_SETTING_KV=dict()
BENCHES=[]
BENCH_OP_ARGS=dict()
BENCH_OP_KV=dict()
BENCH_SETTING_ARGS=dict()
BENCH_SETTING_KV=dict()

TEST_SYSBENCH=False
TEST_CHBENCH=False

BEGIN_TIME=0

MYSQL=0
POSTGRESQL=1
BOTH=2

RESULT_DIR=""
RESULT_BASE=""

STOPPED_OLAP = False

COOLING_TIME=0

SHORT_TABLE_IDX=0
SHORT_TABLE_NUM=0

LONG_TABLE_IDX=0
LONG_TABLE_NUM=0

# Script should be located in proper directory based on below path
DB_BASE=["./MySQL/", "./PostgreSQL/"]

# DB src path ...
DB_SRC_PATH=[DB_BASE[0] + "mysql/", DB_BASE[1] + "postgres/"]

# DB directroy ...
DB_DATA=[base + "data/" for base in DB_BASE] 
DB_CONFIG=[base + "config/" for base in DB_BASE]

# DB script ...
DB_SCRIPT=[base + "script/" for base in DB_BASE]
DB_SERVER_SCRIPT=[base + "script_server/" for base in DB_SCRIPT]
DB_INSTALL_SCRIPT=[base + "install_script_with_pghint.sh" for base in DB_BASE]

# Sysbench ...
SYSBENCH_BASE=[base + "sysbench/" for base in DB_BASE]
SYSBENCH=[base + "sysbench/src/sysbench" for base in SYSBENCH_BASE]
SYSBENCH_LUA=[base + "sysbench/src/lua/" for base in SYSBENCH_BASE]
SYSBENCH_SCRIPT=[base + "sysbench_script/" for base in SYSBENCH_BASE]

# Postgres Resources ...
POST_RSC =[DB_BASE[1] + "data/", DB_BASE[1] + "logfile", DB_BASE[1] + "pgsql/"]

NON_SET_WORKLOAD = ["workload", "run_type"]

class SysbenchWorker(threading.Thread):
    def __init__(self, args):
        threading.Thread.__init__(self)
        self.opts, self.workload = self.setting_params(args)
        print(self.opts)
        self.current_dir = os.getcwd() + "/"

        if DB_TESTBED == 0:
            self.result_file = open(RESULT_DIR + "pgsql_sysbench_vanilla_result", 'w')
        elif DB_TESTBED == 1:
            self.result_file = open(RESULT_DIR + "pgsql_sysbench_ndp_result", 'w')

        print("Sysbench cleanup & prepare start")
        os.system("cd " + SYSBENCH_LUA[RUN_MODE] + "; " + \
                self.current_dir + SYSBENCH[RUN_MODE] + " " + self.opts + \
                " " + self.workload + " cleanup ")
        os.system("cd " + SYSBENCH_LUA[RUN_MODE] + "; " + \
                self.current_dir + SYSBENCH[RUN_MODE] + " " + self.opts + \
                " " + self.workload + " prepare ")

        print("Sysbench cleanup & prepare finish")

    def run(self):
        print("Sysbench worker start !")
        subprocess.run(args=[ 
            self.current_dir + SYSBENCH[RUN_MODE] + " " +  self.opts + \
            " " + self.workload +" run"],
            stdout=self.result_file, stderr=subprocess.STDOUT, check=True,
            cwd=SYSBENCH_LUA[RUN_MODE], shell=True)
        print("Sysbench worker end !")
        self.result_file.close()

    def setting_params(self, args):
        opts=""
        for k, v in vars(args).items():
            if k in NON_SET_WORKLOAD:
                continue
            if RUN_MODE is MYSQL and "pgsql" in k:
                continue
            if RUN_MODE is POSTGRESQL and "mysql" in k:
                continue
            if k == "table_size":
                opts += " --" + k + "=" + v
            if k in SYSBENCH_ARGS:
                opts += " --" + k.replace('_','-') + "=" + v

        workload=args.workload
        return opts, workload

class Client(threading.Thread):
    def __init__(self, long_trx, client_no, args):
        threading.Thread.__init__(self)
        self.client_no = client_no
        self.num_tables = int(args.tables)
        self.long_trx = long_trx
        self.args = args
        if long_trx != 2:
            if DB_TESTBED == 0:
                self.result_file = \
                        open(RESULT_DIR + "pgsql_vanilla_client_" + \
                        str(self.long_trx) + "_" + str(client_no), 'w')
            elif DB_TESTBED == 1:
                self.result_file = \
                        open(RESULT_DIR + "pgsql_ndp_client_" + \
                        str(self.long_trx) + "_" + str(client_no), 'w')

        self.make_connector()
        if long_trx == 0:
            self.query = self.make_short_query()
        elif long_trx == 1:
            self.query = self.make_long_query()
        else:
            self.query = self.make_update_query()
        self.prepare_to_run()

    def prepare_to_run(self):
      '''
        For now, all records visible to the transaction should be in the undo log.
        Therefore, it makes its ReadView firt, and then
        update client updates all records once before executing a join query.
      '''
      if self.long_trx != 1:
        return
      self.db.autocommit=False
      self.cursor.execute("SELECT id FROM sbtest1 where id=1;")
      self.cursor.fetchall()

    def run(self):
        if self.long_trx != 1:
          self.db.autocommit = True

        if self.long_trx == 2:
          # Update
          self.cursor.execute(self.query)
          self.cursor.close()
          self.db.close()
          return
      
        print ("Client " + str(self.client_no) + " start !")
 
        while STOPPED_OLAP == False:
            start_time = time.perf_counter()
            self.cursor.execute(self.query)
            self.cursor.fetchall()
            end_time = time.perf_counter()
            results = f'{(start_time - BEGIN_TIME):.3f} {(end_time - BEGIN_TIME):.3f}\n'
            self.result_file.write(results)
            self.result_file.flush()

        if self.long_trx == 1:
            self.db.commit()

        self.cursor.close()
        self.db.close()

        print("Client " + str(self.long_trx) + "_" + str(self.client_no) + " end !")
        self.result_file.close()

    def make_short_query(self):
        global SHORT_TABLE_IDX
        global SHORT_TABLE_NUM
        '''
        Short transaction query
        Number of tables : 2, 3, 4, 5
        Round-robin manner
        '''
        n_tables = SHORT_TABLE_NUM + 2
        SHORT_TABLE_NUM = (SHORT_TABLE_NUM + 1) % 4

        random_table_numbers = [ (SHORT_TABLE_IDX + i) % self.num_tables + 1 for i in range(0, n_tables)]
        SHORT_TABLE_IDX += n_tables

        random_table_numbers.sort()

        from_stmt = ["sbtest" + str(i) for i in random_table_numbers]
        where_stmt = [i + ".id" for i in from_stmt]
        first_table_id = where_stmt[0]

        #where_stmt = [i + ".k" for i in from_stmt]
        select_stmt = [i + ".k" for i in from_stmt]
        select_stmt = '+'.join(select_stmt)
        select_stmt = 'SUM(' + select_stmt + ')'

        from_stmt = ', '.join(from_stmt)

        where_stmt = \
                [where_stmt[i] + "=" + where_stmt[i+1] \
                for i in range(len(where_stmt) - 1)]

        where_stmt = ' and '.join(where_stmt) + ' and (' + first_table_id + '%2)=0'
        #where_stmt = ' and '.join(where_stmt)
        query = "SELECT " + select_stmt + " FROM " + from_stmt + \
                " WHERE " + where_stmt + ";"
       
        #query = "SELECT COUNT(*) FROM " + from_stmt + \
        #        " WHERE " + where_stmt + ";"

        return query

    def make_long_query(self):
        global LONG_TABLE_IDX
        global LONG_TABLE_NUM

        '''
        Long transaction query
        Number of tables : 
        '''
        table_numbers = [ (i + 1) for i in range(0, self.num_tables) ]

        from_stmt = ["sbtest" + str(i) for i in table_numbers]

        where_stmt = [i + ".id" for i in from_stmt]
        first_table_id = where_stmt[0]

        select_stmt = [i + ".k" for i in from_stmt]
        select_stmt = '+'.join(select_stmt)
        select_stmt = 'SUM(' + select_stmt + ')'
        from_stmt = ', '.join(from_stmt)

        where_stmt = \
                [where_stmt[i] + "=" + where_stmt[i+1] \
                for i in range(len(where_stmt) - 1)]
        where_stmt = ' and '.join(where_stmt) + ' and (' + first_table_id + '%2)=0'

        query = "SELECT " + select_stmt + " FROM " + from_stmt + \
                " WHERE " + where_stmt + ";"
        return query

    def make_update_query(self):
        table_numbers = [ "sbtest" + str(i+1) for i in range(0, self.num_tables) ]

        query = [ "UPDATE " + i + " SET k=k+1" for i in table_numbers ]

        query = ";".join(query)
        return query + ";"

    def make_connector(self):
        self.db = psycopg2.connect(
                host=self.args.pgsql_host,
                dbname=self.args.pgsql_db,
                user=self.args.pgsql_user,
                port=self.args.pgsql_port
                )
        self.cursor = self.db.cursor()

def init_sysbench():
    print("Compile sysbench start")
    subprocess.run(args=[SYSBENCH_SCRIPT[RUN_MODE]+"install.sh"],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            check=True)
    print("Compile sysbench finish")

def create_db():
    print("Create db start")
    subprocess.run(args=[DB_CLIENT_SCRIPT[RUN_MODE]+"create_db.sh"],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
            check=True)
    print("Create db finish")

def copy_database():
    global RUN_MODE
    print("Copy db src start")
    if os.path.isdir(DB_SRC_PATH[RUN_MODE]):
        print("DB src dir already exists")
    else:
        shutil.copytree("../postgres/", DB_SRC_PATH[RUN_MODE])
        print("Copy db src finish")
        
    
def compile_database(params=None):
    print("Compile database start !")
    print(DB_INSTALL_SCRIPT[RUN_MODE], params)
    subprocess.run(args=[DB_INSTALL_SCRIPT[RUN_MODE],params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)
    print("Compile database finish !")

def init_server(params=None):
    print("Init server start !")
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"init_server.sh",
        params],
        check=True)
    print("Init server finish !")

def run_server(params=None):
    print("Run server start")
    print(DB_SERVER_SCRIPT[RUN_MODE]+" run_server.sh"+ " " + params)
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"run_server.sh",
        params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True)
    time.sleep(4)
    print("Run server finish")

def shutdown_server(params=None):
    print("Shutdown server start")
    subprocess.run(args=[DB_SERVER_SCRIPT[RUN_MODE]+"shutdown_server.sh",
        params], 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True)
    time.sleep(4)
    print("Shutdown server finish")

def remove_resource():
    global RUN_MODE
    if RUN_MODE == 1:
        print("Start removing server resource")
        
        shutil.rmtree(DB_BASE[RUN_MODE] + "data/")
        os.remove(DB_BASE[RUN_MODE]+"logfile")
        shutil.rmtree(DB_BASE[RUN_MODE]+"pgsql/")
        shutil.rmtree(DB_BASE[RUN_MODE]+"postgres/")

        print("Finish removing server resource")

def run_standard_benchmark_pgsql(args, db_testbed):
    global BEGIN_TIME
    global RESULT_DIR
    global STOPPED_OLAP
    global STOPPED_SPACE
    global DB_TESTBED
    global SYSBENCH_COMP
    global RESULT_DIR
    global SHORT_TABLE_IDX
    global SHORT_TABLE_NUM
    global LONG_TABLE_NUM
    global LONG_TABLE_IDX
    global TEST_SYSBENCH
    global TEST_CHBENCH
    global DBLIST
    global RUNSRC
    global MODE_ARGS
    global DBS
    global DB_OP_ARGS
    global DB_OP_KV
    global DB_SETTING_ARGS
    global DB_SETTING_KV
    global BENCHES
    global BENCH_OP_ARGS
    global BENCH_OP_KV
    global BENCH_SETTING_ARGS
    global BENCH_SETTING_KV
    global DB_OP_INFO
    global BENCH_OP_INFO


    if db_testbed is "ndp":
        print("PostgreSQ with ndp benchmark start")
        DB_TESTBED = 1
    elif db_testbed is "vanilla":
        print("PostgreSQL-vanilla benchmark start")
        DB_TESTBED = 0
    elif db_testbed is "ndp_non_smartssd":
        print("PostgreSQL with ndp benchmark start in server doesn't have smartssd")

    RESULT_DIR = MODE_ARGS('result_directory')

    if RESULT_DIR == "":
        RESULT_DIR="./results/pgsql_"+ db_testbed + "_" + datetime.datetime.utcnow().strftime("%y-%m-%d_%H:%M:%S") + "/"
    else:
        RESULT_DIR = "./" + RESULT_DIR + "/pgsql_" + db_testbed + "_" +  datetime.datetime.utcnow().strftime("%y-%m-%d_%H:%M:%S") + "/"
  
    pathlib.Path(RESULT_DIR).mkdir(parents=True, exist_ok=True)
  
    with open(RESULT_DIR + "arg_list", 'w') as f:
        json.dump(args.__dict__, f, indent=2)

    if ARGSS['db_build'] == 'yes':
        compile_options=""

        if db_testbed is "ndp":
            compile_options += " --ndp"
        elif db_testbed is "vanilla":
            compile_options += ""
        elif db_testbed is "ndp_non_smartssd":
            compile_options += " --ndp_non_smartssd"
        else:
            assert False

        if ARGSS['build_mode'] == "release":
            compile_options += ""
        elif ARGSS['build_mode'] == "debug0":
            compile_options += " --gdb"
        elif ARGSS['build_mode'] == "debug1":
            compile_options += " --gdb --debugtest"
        else:
            assert False

        if ARGSS['new_config'] == 'yes':
            compile_options += ""
        elif ARGSS['new_config'] == 'no':
            compile_options += " --use_existing_postconf"
        else:
            assert False

        if ARGSS['db_init'] == 'yes':
            compile_options += " --initdb"
        elif ARGSS['db_init'] == 'no':
            compile_options += ""
        else:
            assert False

        if ARGSS['db_create'] == 'yes':
            compile_options += " --createdb"
        elif ARGSS['db_create'] == 'no':
            compile_options += ""
        else:
            assert False

        compile_database(params=compile_options)
        
    else:
        if ARGSS["db_init"] == "yes":
            init_server(params="")
        if ARGSS["new_config"] == "yes":
            shutil.copy(DB_BASE[RUN_MODE] + "postgresql.conf-src", DB_BASE[RUN_MODE] + "postgresql.conf-std")
            with open(DB_BASE[RUN_MODE] + "postgresql.conf-std", "a") as config_file:
                config_file.write("\n\n#------------------------------------------------------------------------------\n")
                config_file.write("# CUSTOMIZED OPTIONS\n")
                config_file.write("#------------------------------------------------------------------------------\n\n")
                config_file.write("# Add settings for extensions here\n")
                config_file.write("smartssd_kernel_file = '/opt/s3d/postgres/postgres-smartssd/binary_container_1.xclbin'")

            # copy postgresql.conf to data dir..
            shutil.copy(DB_BASE[RUN_MODE] + "postgresql.conf-std", DB_BASE[RUN_MODE] + "data/postgresql.conf")
        if ARGSS["db_create"] == "yes":
            create_db()
                

    if ARGSS["run_server"] == "yes":
        run_server(params="--pghint")

    if TEST_SYSBENCH == True:
        if ARGSS["sysbench_build"] == "yes":
            SYSBENCH_COMP = False
        elif ARGSS["sysbench_build"] == "no":
            SYSBENCH_COMP = True
        else:
            assert False

        # Compile sysbench code
        if SYSBENCH_COMP is False:
            init_sysbench()
            SYSBENCH_COMP = True

        SHORT_TABLE_IDX=0
        SHORT_TABLE_NUM=0

        LONG_TABLE_IDX=0
        LONG_TABLE_NUM=0

        STOPPED_OLAP=False

        # Make sysbench workers
        sys_worker = SysbenchWorker(args)

        # Make clients (reader)
        olap_client = [Client(long_trx=1, client_no=i, args=args) for i in range(0, int(args.num_long_olap))]

        # Do updates all tables once !
        print("Start updating all records")
        update_client = Client(long_trx=2, client_no=0, args=args)
        update_client.start()
        update_client.join()
        print("Finish updating all records")

        # Run sysbench worker
        sys_worker.start()
        # wait for warmup time if needed
        if int(args.warmup_time) != 0:
            time.sleep(int(args.warmup_time))

        BEGIN_TIME = time.perf_counter()

        time.sleep(int(args.olap_wait_time)) # Default: olap_wait_time: 0 sec

        print("Start client workers")
        for client in olap_client:
            client.start()

        # Third phase : Run OLAP ONLY
        while time.perf_counter() - BEGIN_TIME < float(args.time):
            time.sleep(0.5)

        STOPPED_OLAP = True

        for client in olap_client:
            client.join()

        sys_worker.join()

    if db_testbed is "ssd":
      print("PostgreSQLwith ndp benchmark done")
    else:
      print("PostgreSQL-vanilla benchmark done")

    if ARGSS['shutdown_server'] == 'yes':
        shutdown_server(params="")

    if ARGSS['remove_resource'] == 'yes':
        remove_resource()

    time.sleep(5)

def preprocessing_args(args):
    global TEST_SYSBENCH
    global TEST_CHBENCH
    global DBLIST
    global RUNSRC
    global MODE_ARGS
    global DBS
    global DB_OP_ARGS
    global DB_OP_KV
    global DB_SETTING_ARGS
    global DB_SETTING_KV
    global BENCHES
    global BENCH_OP_ARGS
    global BENCH_OP_KV
    global BENCH_SETTING_ARGS
    global BENCH_SETTING_KV
    global DB_OP_INFO
    global BENCH_OP_INFO

    ARGSS = vars(args)
    
    if ARGSS['is_first']:
        ARGSS['run_server'] = ARGSS['db_create'] = ARGSS['new_config'] = ARGSS['db_init'] = ARGSS['db_build'] = ARGSS['ch_build'] = ARGSS['sysbench_build'] = ARGSS['is_first']

    if ARGSS['db_run_mode'] == 'all':
        DBS=MODE_ARGS['db_run_mode'] = DBLIST
    else:
        if ARGSS['db_run_mode'] not in DBLIST:
            print("TEST BE SHOULD BE IN DBLIST") 
            print(DBLIST)
            assert False
        DBS=MODE_ARGS['db_run_mode'] = [ARGSS['db_run_mode']]

    if ARGSS['run_type'] == 'vanilla':
        MODE_ARGS['run_type'] = ['vanilla'] 
    elif ARGSS['run_type'] == 'ndp':
        MODE_ARGS['run_type'] = ['ndp']
    elif ARGSS['run_type'] == 'ndp_non_smartssd':
        MODE_ARGS['run_type'] = ['ndp_non_smartssd']
    else:
        assert False

    MODE_ARGS['result_directory'] = ARGSS['result_directory']
    MODE_ARGS['is_first'] = ARGSS['is_first']

    for db in DBS:
        DB_OP_ARGS[db] = []
        DB_OP_KV[db] = dict()
        for db_op in DB_OP_INFO:
            DB_OP_KV[db][db_op] = []
        DB_SETTING_ARGS[db] = []
        DB_SETTING_KV[db] = dict()

        DB_OP_KV_DICT = DB_OP_KV[db]
        DB_SETTING_KV_DICT = DB_SETTING_KV[db]

        if ARGSS['db_build']:
            DB_OP_ARGS[db].append('build')

        if ARGSS['debug_level'] == "0":
            DB_OP_KV_DICT['build'].append('release')
        elif ARGSS['debug_level'] == "1":
            DB_OP_KV_DICT['build'].append('debug0')
        elif ARGSS['debug_level'] == "2":
            DB_OP_KV_DICT['build'].append('debug1')

        DB_OP_KV_DICT['build'].append(ARGSS['compile_option'])
        
        if ARGSS['db_init']:
            DB_OP_ARGS[db].append('init')
        
        if ARGSS['new_config']:
            DB_OP_KV_DICT['init'].append("new_config")

        if ARGSS['db_create']:
            DB_OP_ARGS[db].append('create')

        if ARGSS['run_server']:
            DB_OP_ARGS[db].append('run')

        if ARGSS['shutdown_server']:
            DB_OP_ARGS[db].append('shutdown')

        if ARGSS['remove_resource']:
            DB_OP_ARGS[db].append('purge')

        for db_param in DB_ARGS:
            DB_SETTING_ARGS[db].append(db_param) 
            DB_SETTING_KV_DICT[db_param] = ARGSS[db_param]

    if ARGSS['benchmark_type'] == 'sysbench':
        BENCHES = MODE_ARGS['benchmark_type'] = ['sysbench']
    elif ARGSS['benchmark_type'] == 'chbench':
        BENCHES = MODE_ARGS['benchmark_type'] = ['chbench']
    elif ARGSS['benchmark_type'] == 'all':
        BENCHES = MODE_ARGS['benchmark_type'] = BENCHLIST
    else:
        assert False

    for bench in BENCHES:
        BENCH_OP_ARGS[bench] = []
        BENCH_OP_KV[bench] = dict()
        for bench_op in BENCH_OP_INFO[bench]:
            BENCH_OP_KV[bench][bench_op] = []
        BENCH_SETTING_ARGS[bench] = []
        BENCH_SETTING_KV[bench] = dict()
        
        BENCH_OP_KV_DICT = BENCH_OP_KV[bench]
        BENCH_SETTING_KV_DICT = BENCH_SETTING_KV[bench]

        if bench == "sysbench":
            if ARGSS['sb_init']:
                BENCH_OP_ARGS[bench].append('init')
                BENCH_OP_KV_DICT['init'] = "YES"
            if ARGSS['sb_run']:
                BENCH_OP_ARGS[bench].append('run')
                BENCH_OP_KV_DICT['run'] = "YES"

            for sys_param in SYSBENCH_ARGS:
                # in case of db-driver, set to process time
                if sys_param == "db_driver":
                    BENCH_SETTING_ARGS[bench].append(sys_param)
                    BENCH_SETTING_KV_DICT[sys_param] = ''
                    continue
                BENCH_SETTING_ARGS[bench].append(sys_param)
                BENCH_SETTING_KV_DICT[sys_param] = ARGSS[sys_param]
    
        if bench == "chbench":
            if ARGSS['ch_init']:
                BENCH_OP_ARGS[bench].append('init')
                BENCH_OP_KV_DICT['init'] = "YES"
            if ARGSS['ch_run_citus']:
                BENCH_OP_ARGS[bench].append('run_citus')
                BENCH_OP_KV_DICT['run_citus'] = "YES"
            if ARGSS['ch_run_psql']:
                BENCH_OP_ARGS[bench].append('run_psql')
                BENCH_OP_KV_DICT['run_psql'] = "YES"
                
            for ch_param in CHBENCH_ARGS:
                BENCH_SETTING_ARGS[bench].append(ch_param)
                BENCH_SETTING_KV_DICT[ch_param] = ARGSS[ch_param]


    print("MODE_ARGS : ")
    print(MODE_ARGS)
    print("DBS : ")
    print(DBS)
    print("DB_OP_ARGS : ")
    print(DB_OP_ARGS)
    print("DB_OP_KV : ")
    print(DB_OP_KV)
    print("DB_SETTING_ARGS : ")
    print(DB_SETTING_ARGS)
    print("DB_SETTING_KV : ")
    print(DB_SETTING_KV)
    print("BENCHES : ")
    print(BENCHES)
    print("BENCH_OP_ARGS : ")
    print(BENCH_OP_ARGS)
    print("BENCH_OP_KV : ")
    print(BENCH_OP_KV)
    print("BENCH_SETTING_ARGS : ")
    print(BENCH_SETTING_ARGS)
    print("BENCH_SETTING_KV : ")
    print(BENCH_SETTING_KV)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Microbench Arguments...")

    global_options_parser = parser.add_argument_group('options', 'Expr setting')
    pgsql_parser = parser.add_argument_group('pgsql', 'postgresql options')
    sysbench_parser = parser.add_argument_group('sysbench', 'sysbench options')
    chbench_parser = parser.add_argument_group('chbench', 'chbench options')

    global_options_parser.add_argument("--db_run_mode", default="pgsql", help="current test is for pgsql only")
    global_options_parser.add_argument("--db_build", action="store_true", help="if yes, then build db")
    global_options_parser.add_argument("--debug_level", default="0", help="0 : release mode 1 : debug mode without test code 2: debug mode with test code")
    global_options_parser.add_argument("--run_type", default="vanilla", help="vanilla: vanilla, ndp: ndp, ndp_non_smartssd : for test, build AIDE MODE  in servers without smartssd ")
    global_options_parser.add_argument("--benchmark_type", default="all", help="sysbench: sysbench, chbench: chbench, all: test sysbench and chbench")
    global_options_parser.add_argument("--compile_option", default='', help="compile options")
    global_options_parser.add_argument("--result_directory", default='', help="target directory of result")
    global_options_parser.add_argument("--sysbench_build", action="store_true", help="compile sysbenchmark")
    global_options_parser.add_argument("--is_first", action="store_true", help="Is it first execution?")
    global_options_parser.add_argument("--db_init", action="store_true", help="do initializing db or not")
    global_options_parser.add_argument("--new_config", action="store_true", help="do creating new config file or not")
    global_options_parser.add_argument("--db_create", action='store_true', help="do creating new database or not")
    global_options_parser.add_argument("--run_server", action='store_false', help="do running database server or not")
    global_options_parser.add_argument("--shutdown_server", action='store_false', help="shutdown database server or not")
    global_options_parser.add_argument("--remove_resource", action='store_true', help="remove database resource or not")

    pgsql_parser.add_argument("--pgsql-superuser", default="$CUR_USER", help="postgres super user")
    pgsql_parser.add_argument("--pgsql-port", default="7777", help="postgres server port number")
    pgsql_parser.add_argument("--pgsql-superpass", default="postgres", help="postgres super user password")
    pgsql_parser.add_argument("--pgsql-host", default="localhost", help="host ip of database server")

    sysbench_parser.add_argument("--sb_init", action="store_true", help="do building sysbnech tables or not")
    sysbench_parser.add_argument("--sb_run", action="store_false", help="do running sysbench or not")
    sysbench_parser.add_argument("--pgsql-db", default="sbtest", help="database name for sysbench experiment")
    sysbench_parser.add_argument("--pgsql-user", default="sbtest", help="user for sysbench experiment")
    sysbench_parser.add_argument("--pgsql-password", default="sbtest", help="user password for sysbench experiment")

    sysbench_parser.add_argument("--db-driver", default="pgsql", help="current test is for pgsql only")
    sysbench_parser.add_argument("--report-interval", default="1", help="report interval. default 1 seconod")
    sysbench_parser.add_argument("--secondary", default="off", help="default off")
    sysbench_parser.add_argument("--create-secondary", default="false", help="default = false")
    sysbench_parser.add_argument("--time", default="60", help="total execution time")
    sysbench_parser.add_argument("--threads", default="12", help="number of threads")
    sysbench_parser.add_argument("--tables", default="12", help="number of tables")
    sysbench_parser.add_argument("--table-size", default="10000", help="sysbench table size")
    sysbench_parser.add_argument("--warmup-time", default="0", help="sysbench warmup time")
    sysbench_parser.add_argument("--rand-type", default="zipfian")
    sysbench_parser.add_argument("--rand-zipfian-exp", default="0.0")
    sysbench_parser.add_argument("--workload", default="oltp_update_non_index.lua",\
            help="lua script. default : oltp update non index")
    sysbench_parser.add_argument("--num_long_olap", default="1")
    sysbench_parser.add_argument("--olap_wait_time", default="20")
    
    chbench_parser.add_argument("--ch_init", action="store_true", help="build chbench table (yes or no)")
    chbench_parser.add_argument("--ch_run_citus", action="store_true", help="running citus benchmark or not")
    chbench_parser.add_argument("--ch_run_psql", action="store_false", help="running psql client or not")
    chbench_parser.add_argument("--warehouse", default="2", help="number of warehouse in chbenchmark")
    chbench_parser.add_argument("--runclient", default="no", help="run chbenchmark client for test")
    chbench_parser.add_argument("--ch_istpcc", default="no", help="run tpcc or not")
    chbench_parser.add_argument("--ch_isch", default="yes", help="run tpch or not")

    chbench_parser.add_argument("--ch_db", default="chbench", help="database name for sysbench experiment")
    chbench_parser.add_argument("--ch_user", default="chbench", help="user for sysbench experiment")
    chbench_parser.add_argument("--ch_pass", default="chbench", help="user password for sysbench experiment")

    chbench_parser.add_argument("--ch_runtime", default="1800", help="chbench runtime per sec")
    chbench_parser.add_argument("--ch_rampuptime", default="1", help="ramp up time of chbenchmark")
    chbench_parser.add_argument("--ch_thread_cnt", default="1", help="number of worker")
    chbench_parser.add_argument("--ch_run_vu", default="2", help="number of tpcc worker")
    
    args=parser.parse_args()
    preprocessing_args(args)

'''
    if args.run_type == "vanilla":
        run_standard_benchmark_pgsql(args, "vanilla")
    elif args.run_type == "ndp":
        run_standard_benchmark_pgsql(args, "ndp")
    elif args.run_type == "ndp_non_smartssd":
        run_standard_benchmark_pgsql(args, "ndp_non_smartssd")
    else:
        assert False
'''
