import psycopg2
import sys, subprocess
import time, threading
import os, random
import argparse
import pathlib, datetime
import json
import fileinput
import random
import re
import glob
import multiprocessing

import mysql.connector

# TODO
# - config file for the NDP needs kernel path

# Run mode
VANILLA = 0
AIDE = 1
BOTH = 2

SHORT_TRX = 0
LONG_TRX = 1

def log(s):
    print('[Microbench] {}'.format(s))

def run_script(script, params, desc, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT):
    command = [script]
    command.extend(params)
    log('{} start!'.format(desc))
    result = subprocess.run(args=command,
                            stdout=stdout, stderr=stderr,
                            check=True)
    log('{} finished!'.format(desc))

def create_config_file(config_file, new_config_file, args):
    replacers = [['port=', 'port={}'.format(args.mysql_port)],
                 ['basedir=', 'basedir={}'.format(args.inst_dir)],
                 ['datadir=', 'datadir={}'.format(args.data_dir)],
                 ['socket=', 'socket={}'.format(args.mysql_socket)],
                 ['general_log_file=', 'general_log_file={}'.format(args.general_logfile)],
                 ['log_error=', 'log_error={}'.format(args.error_logfile)]]
    if args.run_mode == AIDE:
        replacers.append(['s3d_kernel_name=', 's3d_kernel_name={}'.format(args.kernel)])

    # After init server, the config file is copied into the data directory
    with open(new_config_file, 'w') as new_config:
        with open(config_file, 'r') as src_config:
            for line in src_config:
                if line[0] == '#':
                    continue
                for replacer in replacers:
                    if replacer[0] in line:
                        line = replacer[1] + '\n'
                new_config.write(line)


# TPS extract
def lines_that_contain(string, fp):
    return [line for line in fp if string in line]

def extract_tps(lines):
    tps_regex = re.compile(r'tps: (\d+\.\d+)')
    str_result = [tps_regex.search(line).group(1) for line in lines]
    result = [float(tps) for tps in str_result]
    return result

def parse_tps(infile, outfile):
    parsed_lines = []
    with open(infile, 'r') as fp:
        parsed_lines.extend(lines_that_contain('tps:', fp))

    tps_list = extract_tps(parsed_lines)

    with open(outfile, 'w') as fp:
        for tps in tps_list:
            fp.write('%s\n' % tps)
        fp.write('0') # for graph consistency


# Sysbench
class SysbenchWorker(threading.Thread):
    def __init__(self, sysbench_script, sysbench_dir, result_file, args):
        threading.Thread.__init__(self)

        self.sysbench_script = sysbench_script
        self.result_file = result_file
        self.args = args
        self.options = self.parse_params(args)

        self.sysbench_dir = sysbench_dir

        src_dir = os.path.join(sysbench_dir, 'src')
        lua_file = os.path.join(src_dir, 'lua', args.lua)

        self.params = ['--sysbench-dir={}'.format(sysbench_dir),
                       '--lua={}'.format(lua_file)]
        self.params.extend(self.options)

        params = self.params.copy()
        params.append('--mode=cleanup')
        run_script(self.sysbench_script, params, 'Sysbench cleanup')

        params = self.params.copy()
        params.append('--mode=prepare')
        run_script(self.sysbench_script, params, 'Sysbench prepare')

    def run(self):
        params = self.params.copy()
        params.append('--mode=run')
        with open(self.result_file, 'w') as f:
            run_script(self.sysbench_script, params, 'Sysbench run', stdout=f, stderr=subprocess.DEVNULL)

    def parse_params(self, args):
        options = []
        for k, v in vars(args).items():
            if 'lua' not in k:
                options.append('--{}={}'.format(k,str(v)).replace('_', '-'))
        return options

# Client for running long joins on sysbench
class Client(threading.Thread):
    def __init__(self, client_id, autocommit, result_file, args):
        threading.Thread.__init__(self)

        self.client_id = client_id
        self.autocommit = autocommit
        self.result_file = result_file
        self.args = args

        self.tables = int(args.tables)
        self.join_tables = int(args.join_tables)

        self.db = self.make_connector()
        self.cursor = self.db.cursor()
        self.end = False

        print(client_id, autocommit, self.make_query())

    def terminate(self):
        if self.end is False:
            self.end = True

    def run(self):
        with open(self.result_file, 'w') as f:
            epoch = time.perf_counter()

            if self.is_aide():
                self.cursor.execute('set @@session.ndp_src_table="SBTEST1"')
                self.cursor.execute('set @@session.s3d_forced_table_scan=TRUE')

            if self.args.run_mode == AIDE:
                self.cursor.execute('SELECT @@session.ndp_src_table')
                session_variable = self.cursor.fetchall()
                print(self.client_id, self.autocommit, session_variable)

            if not self.autocommit:
                self.db.start_transaction()

            while not self.end: # ends looping in terminate()
                start_time = time.perf_counter()

                query = self.make_query()
                self.cursor.execute(query)
                self.cursor.fetchall()

                end_time = time.perf_counter()

                results = f'{(start_time - epoch + self.args.time_oltp_only):.3f} {(end_time - epoch + self.args.time_oltp_only):.3f}\n'
                f.write(results)
                f.flush()

            if not self.autocommit:
                self.db.commit()

            self.cursor.close()
            self.db.close()

    def make_query(self):
        rand_tables = random.sample(range(1, self.tables + 1), self.join_tables)

        rand_tables.sort()

        if self.is_aide():
            if 1 not in rand_tables:
                rand_tables[0] = 1

        from_stmt = ['sbtest{}'.format(str(i)) for i in rand_tables] # sbtest1
        where_stmt = ['{}.k'.format(i) for i in from_stmt] # sbtest1.k
        if self.args.with_predicate:
            predicate_value = int(int(args.table_size) * args.selectivity)
            predicate = ['{}.k <= {}'.format(i, predicate_value) for i in from_stmt[:1]]
            predicate = ' and '.join(predicate)

        select_stmt = [i + '.k' for i in from_stmt]
        select_stmt = ' + '.join(select_stmt)
        select_stmt = 'SUM({})'.format(select_stmt)

        from_stmt = ', '.join(from_stmt)

        where_stmt = \
                ['{} = {}'.format(where_stmt[i], where_stmt[i+1]) \
                for i in range(len(where_stmt) - 1)]
        where_stmt = ' and '.join(where_stmt)
        if self.args.with_predicate and self.client_id == 0 and self.autocommit == False:
            where_stmt = '{} and {}'.format(where_stmt, predicate)

        query = 'SELECT STRAIGHT_JOIN {} FROM {} WHERE {};'.format(select_stmt, from_stmt, where_stmt)

        return query

    def make_connector(self):
        return mysql.connector.connect(
                host=self.args.mysql_host,
                database=self.args.mysql_db,
                user=self.args.mysql_user,
                port=self.args.mysql_port,
                autocommit=True) # we wrap long olap with begin/commit so autocommit doesn't matter
                #autocommit=self.autocommit)

    def is_aide(self):
        return self.args.run_mode == AIDE and self.client_id == 0 and self.autocommit == False


# Space checker for data size
class SpaceChecker(threading.Thread):
    def __init__(self, result_file, data_dir, args):
        threading.Thread.__init__(self)

        self.result_file = result_file
        self.data_dir = data_dir
        self.args = args

        self.end = False

    def terminate(self):
        if self.end is False:
            self.end = True

    def run(self):
        default_size = int(str(subprocess.check_output([f'du -sbc {self.data_dir}/.rocksdb/*.sst*'], shell=True).decode('utf-8')).split()[-2])

        with open(self.result_file, 'w') as f:
            epoch = time.perf_counter()

            while not self.end: # ends looping in terminate()
                current_time = time.perf_counter()

                try:
                    data_size = int(str(subprocess.check_output([f'du -sbc {self.data_dir}/.rocksdb/*.sst*'], shell=True).decode('utf-8')).split()[-2])
                except Exception:
                    pass

                #results = f'{(current_time - epoch):.3f} {data_size - default_size}\n'
                results = f'{(current_time - epoch):.3f} {data_size}\n'
                f.write(results)
                f.flush()
                time.sleep(1)


def run_exp(args, mode):
    # Directory paths
    base_dir       = os.getcwd()
    sysbench_base  = os.path.join(base_dir, 'sysbench')
    src_dir        = os.path.join(base_dir, 'myrocks')
    inst_dir       = os.path.join(base_dir, 'inst')
    data_dir       = os.path.join(inst_dir, 'data')
    config_dir     = os.path.join(base_dir, 'config')
    result_base    = os.path.join(base_dir, 'results')
    script_dir     = os.path.join(base_dir, 'scripts')
    sysbench_dir   = os.path.join(base_dir, 'sysbench')
    result_dir     = os.path.join(result_base, datetime.datetime.utcnow().strftime('%y-%m-%d_%H:%M:%S'))
    recent_dir     = os.path.join(result_base, 'recent')
    bin_dir        = os.path.join(inst_dir, 'bin')
    lib_dir        = os.path.join(inst_dir, 'lib')
    perf_dir       = os.path.join(base_dir, 'logs')

    # File paths
    args_dump = os.path.join(result_dir, 'args.txt')
    config_file = os.path.join(config_dir, 'my.cnf.{}'.format(mode))
    new_config_file = os.path.join(inst_dir, 'my.cnf')
    install_script = os.path.join(script_dir, 'install.sh')
    sysbench_install_script = os.path.join(script_dir, 'install_sysbench.sh')
    sysbench_script = os.path.join(script_dir, 'sysbench.sh')
    plot_script = os.path.join(script_dir, 'plot.sh')
    init_script = os.path.join(script_dir, 'init_server.sh')
    start_script = os.path.join(script_dir, 'start_server.sh')
    create_script = os.path.join(script_dir, 'create_db.sh')
    stop_script = os.path.join(script_dir, 'stop_server.sh')
    general_logfile = os.path.join(data_dir, 'general.log')
    error_logfile = os.path.join(data_dir, 'error.log')
    sysbench_file = os.path.join(result_dir, 'sysbench.data')
    tps_file = os.path.join(result_dir, 'tps.data')
    space_file = os.path.join(result_dir, 'space.data')
    eps_file = os.path.join(result_dir, 'result.eps')
    gpi_file = os.path.join(script_dir, 'sysbench.gpi')
    pid_file = os.path.join(data_dir, 'multicore-{}.pid'.format(multiprocessing.cpu_count()))
    kernel_file = os.path.join(config_dir, 'binary_container_1.xclbin')
    mysql_socket = os.path.join(inst_dir, 'mysql.sock')

    # Create result directory
    pathlib.Path(result_dir).mkdir(parents=True, exist_ok=True)

    # Create a link of result directory to 'recent' directory for convenience
    os.system("rm -rf {}".format(recent_dir))
    os.system("ln -rs {} {}".format(result_dir, recent_dir))

    log('{} experiment start!'.format(mode))

    # Compile sysbench code
    if args.install_sysbench:
        params = ['--sysbench-base-dir={}'.format(sysbench_base)]
        run_script(sysbench_install_script, params, 'Compile sysbench', stdout=None)

    # Install myrocks (i.e., compile, install, etc.)
    if args.run_mode == AIDE:
        args.compile_option += "-DSMARTSSD -DWITH_NDP_LIB -DVERSION_INDEX -DSMARTSSD_FLUSH_MEMTABLE_OPT -DSMARTSSD_FLUSH_MEMTABLE"
        #args.lib_option += '-L/opt/xilinx/xrt/lib -lndpf -lstdc++ -lxilinxopencl'
        if args.gdb:
            args.compile_option += ' -DVERSION_INDEX_DEBUG'

    if args.install:
        params = ['--src-dir={}'.format(src_dir),
                  '--inst-dir={}'.format(inst_dir),
                  '--data-dir={}'.format(data_dir),
                  #'--lib-option={}'.format(args.lib_option),
                  '--compile-option={}'.format(args.compile_option),
                  '--mysql-port={}'.format(args.mysql_port)]
        if args.gdb:
            params.append('--gdb')
        run_script(install_script, params, desc='Compile database', stdout=None)

        # Stop install after first install
        args.install = False

    # Create a new config file according to the args
    if args.run_mode == AIDE:
        os.system('cp {} {}'.format(kernel_file, inst_dir))
        args.kernel = os.path.join(inst_dir, 'binary_container_1.xclbin')

    # Required information for creating the config file
    args.inst_dir = inst_dir
    args.data_dir = data_dir
    args.general_logfile = general_logfile
    args.error_logfile = error_logfile
    args.mysql_socket = mysql_socket

    # Dump args
    with open(args_dump, 'w') as f:
        json.dump(args.__dict__, f, indent=2)

    create_config_file(config_file, new_config_file, args)

    # Leave a copy of the config file to result directory
    os.system('cp {} {}'.format(new_config_file, result_dir))

    # Init DB (i.e., mysql_install_db)
    if args.initdb:
        params = ['--inst-dir={}'.format(inst_dir),
                  '--data-dir={}'.format(data_dir),
                  '--config-file={}'.format(new_config_file),
                  '--pid-file={}'.format(pid_file)]
        run_script(init_script, params, desc='Init DB')

    # Start server
    params = ['--inst-dir={}'.format(inst_dir),
              '--data-dir={}'.format(data_dir),
              '--config-file={}'.format(new_config_file),
              '--pid-file={}'.format(pid_file),
              '--error-logfile={}'.format(error_logfile),
              '--skip-stack-trace']
    if args.gdb:
        params.append('--gdb')
    if args.cgroup:
        params.append('--cgroup')

    run_script(start_script, params, desc='Start server')

    time.sleep(5)

    # Sysbench
    sys_worker = SysbenchWorker(sysbench_script=sysbench_script, sysbench_dir=sysbench_dir, result_file=sysbench_file, args=args)

    space_checker = SpaceChecker(result_file=space_file, data_dir=data_dir, args=args)
    short_client = [Client(client_id=i, autocommit=True, result_file=os.path.join(result_dir, f'latency_short_{i}.data'), args=args) \
            for i in range(0, args.num_short_olap)]
    long_client = [Client(client_id=i, autocommit=False, result_file=os.path.join(result_dir, f'latency_long_{i}.data'), args=args) \
            for i in range(0, args.num_long_olap)]

    log('<Phase 1> Warmup ({} sec) -> OLTP only ({} sec)'.format(args.warmup_time, args.time_oltp_only))
    sys_worker.start()
    space_checker.start()
    if args.warmup_time != 0:
        time.sleep(args.warmup_time)

    time.sleep(args.time_oltp_only)

    both_time = args.time - (args.time_oltp_only + args.time_olap_only)
    log('<Phase 2> Run OLAP + OLTP together ({} sec)'.format(both_time))

    for client in short_client:
        client.start()
    for client in long_client:
        client.start()
    time.sleep(both_time)

    log('<Phase 3> Run OLAP only ({} sec)'.format(args.time_olap_only))
    time.sleep(args.time_olap_only)

    log('Waiting for OLAP clients to terminate...')
    sys_worker.join()
    space_checker.terminate()
    space_checker.join()

    # Parse TPS from results/recent/sysbench.data file
    parse_tps(sysbench_file, tps_file)

    for client in short_client:
        client.terminate()
    for client in long_client:
        client.terminate()
    for client in short_client:
        client.join()
    for client in long_client:
        client.join()

    # Stop server
    params = ['--inst-dir={}'.format(inst_dir)]
    run_script(stop_script, params, desc='Stop server')

    # Plot using the result files
    params = ['--gpi={}'.format(gpi_file),
              '--eps-file={}'.format(eps_file),
              '--latency-short-file={}'.format(short_client[0].result_file),
              '--latency-long-file={}'.format(long_client[0].result_file),
              '--tps-file={}'.format(tps_file),
              '--space-file={}'.format(space_file)]
    run_script(plot_script, params, desc='Plot data')

    # Copy necessary files
    os.system('cp {} {}'.format(error_logfile, result_dir))
    #os.system('cp {} {}'.format(general_logfile, result_dir))

    #if args.run_mode == AIDE:
    #    perf_files = glob.glob(os.path.join(perf_dir, '*'))
    #    latest_perf_file = max(perf_files, key=os.path.getctime)
    #    os.system('cp {} {}'.format(latest_perf_file, os.path.join(result_dir, 'profile.data')))

    selectivity_result_dir = os.path.join(base_dir, 'selectivity', mode, str(int(args.selectivity * 100)))
    pathlib.Path(selectivity_result_dir).mkdir(parents=True, exist_ok=True)
    recent_files = os.path.join(recent_dir, '*')
    os.system('cp -r {} {}'.format(recent_files, selectivity_result_dir))

    log('{} experiment finished!'.format(mode))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sysbench Arguments...')

    # Parser for each context
    mysql_parser = parser.add_argument_group('mysql', 'mysql options')
    sysbc_parser = parser.add_argument_group('sysbench', 'Sysbench options')
    options_parser = parser.add_argument_group('options', 'Other options')

    # MySQL
    sysbc_parser.add_argument("--db-driver", default="mysql", help="mysql or pgsql")
    mysql_parser.add_argument("--mysql-host", default="localhost", help="mysql host")
    mysql_parser.add_argument("--mysql-db", default="sbtest", help="mysql database")
    mysql_parser.add_argument("--mysql-user", default="root", help="mysql user")
    mysql_parser.add_argument("--mysql-port", default="3789", help="mysql port number")

    # Sysbench
    sysbc_parser.add_argument('--install-sysbench', default=False, action='store_true', help='intall sysbench?')
    sysbc_parser.add_argument('--report-interval', default=1, help='report interval. default 1 second')
    sysbc_parser.add_argument('--secondary', default='off', help='default off')
    sysbc_parser.add_argument('--create-secondary', default='false', help='default = false')
    sysbc_parser.add_argument('--time', type=int, default=3600, help='total execution time')
    sysbc_parser.add_argument('--threads', type=int, default=48, help='number of threads')
    sysbc_parser.add_argument('--tables', type=int, default=8, help='number of tables')
    sysbc_parser.add_argument('--join-tables', type=int, default=4, help='number of tables to join for each query')
    sysbc_parser.add_argument('--table-size', type=int, default=10000, help='sysbench table size')
    sysbc_parser.add_argument('--warmup-time', type=int, default=0, help='sysbench warmup time')
    sysbc_parser.add_argument('--rand-type', default='zipfian')
    sysbc_parser.add_argument('--rand-zipfian-exp', default='0.0')
    sysbc_parser.add_argument('--lua', default='oltp_update_non_index.lua',\
            help='lua script, default: oltp_update_non_index')

    # Others
    options_parser.add_argument('--install', default=False, action='store_true', help='execute reinstallation or not')
    options_parser.add_argument('--run-mode', type=int, default=0, help='0: Vanilla, 1: AIDE, 2: both')
    options_parser.add_argument('--compile-option', default='', help='compile options')
    options_parser.add_argument('--lib-option', default='', help='lib options')
    options_parser.add_argument('--gdb', default=False, action='store_true', help='compile with gdb enabled')
    options_parser.add_argument('--initdb', default=False, action='store_true', help='initialize a new database')
    options_parser.add_argument('--cgroup', default=False, action='store_true', help='execute within cgroup')

    options_parser.add_argument('--time-olap-only', type=int, default=600, help='OLAP exclusive time')
    options_parser.add_argument('--time-oltp-only', type=int, default=600, help='OLTP exclusive time')
    options_parser.add_argument('--num-short-olap', type=int, default=10, help='# of OLAP queries w\o autocommit')
    options_parser.add_argument('--num-long-olap', type=int, default=10, help='# of OLAP queries w\ autocommit')
    options_parser.add_argument('--with-predicate', default=False, action='store_true', help='put predicate for selectivity')

    args=parser.parse_args()

    if args.run_mode == VANILLA:
        mode_str = 'vanilla'
    elif args.run_mode == AIDE:
        mode_str = 'aide'
    else:
        exit(-1)

    #selectivities = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    selectivities = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    #selectivities = [0.1]

    for selectivity in selectivities:
        log('{} experiment start!'.format(str(selectivity)))
        args.selectivity = selectivity
        run_exp(args, mode_str)
        log('{} experiment finished!'.format(str(selectivity)))

