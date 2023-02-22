#!/usr/bin/python3

import os
import sys
import random
import atexit
import signal
import subprocess
import mysql.connector
import time, threading
import pathlib
from datetime import datetime

import json
import fileinput
import re

current_workdir = os.getcwd()
aide_queries_path = current_workdir + "/ch-queries/q"
vanilla_queries_path = current_workdir + "/ch-queries/q"
c_view_path = current_workdir + "/ch-queries/c_view.sql"

sent_query_amount = 0
is_terminated = False
file_suffix="0"

RANDOM_SEED = 123
AIDE = False
num_ch_query = 22

ch_vanilla_queries_path = [vanilla_queries_path + str(x) + ".sql" for x in range(1,23)]

class PlotWorkerBarrier():
    def __init__(self, guard_val):
        self.lock = threading.Lock()
        self.exec_epoch = 0
        self.exec_cnt = 0
        self.guard_val = guard_val

    def ok_to_go(self, worker_epoch):
        ret_epoch = 0

        self.lock.acquire()
       
        if worker_epoch == self.exec_epoch:
            ret_epoch = self.exec_epoch + 1
        else:
            ret_epoch = -1
        
        self.lock.release()

        return ret_epoch

    def exec_fin(self):
        self.lock.acquire()
        self.exec_cnt += 1
        self.exec_epoch = self.exec_cnt // self.guard_val
        self.lock.release()


# Client for running long joins on sysbench
class Client(threading.Thread):
    def __init__(self, client_id, autocommit, result_file, aide, group, group_query, plot):
        global ch_vanilla_queries_path
        global aide_queries_path

        threading.Thread.__init__(self)

        self.client_id = client_id
        self.autocommit = autocommit
        self.result_file = result_file
        self.aide = aide
        self.group = group
        self.group_query = group_query

        self.db = self.make_connector()
        self.cursor = self.db.cursor()
        self.end = False
        self.group = group

        self.ch_queries = []
        self.query_num = 22
        self.group_query_num = []
        self.ch_aide_queries_path = []

        self.plot_worker = plot

        if self.aide:
            if self.group == 0:
                self.query_num = 22
                self.query_seq =[x for x in range(1,23)]
                self.ch_aide_queries_path = [aide_queries_path + str(x) + ".sql" for x in range(1,23)]
            elif self.group == 1:
                self.query_num = 7
                self.query_seq = [2, 5, 8, 9, 11, 21, 20]
            elif self.group == 2:
                self.query_num = 2
                self.query_seq = [15, 16]
            elif self.group == 3:
                self.query_num = 7
                self.query_seq = [3, 10, 12, 14, 17, 18, 19]
            elif self.group == 4:
                self.query_num = 5
                self.query_seq = [1, 4, 6, 13, 22]
            else:
                assert 0, "input incorrect group\n"

            self.ch_aide_queries_path = [aide_queries_path + str(x) + ".sql" for x in self.query_seq]

            for query_path in self.ch_aide_queries_path:
                f_query = open(query_path, 'r')
                f_contents = f_query.read()
                self.ch_queries.append(f_contents)
                f_query.close()
        else:
            for query_path in ch_vanilla_queries_path:
                f_query = open(query_path, 'r')
                f_contents = f_query.read()
                self.ch_queries.append(f_contents)
                f_query.close()

    def terminate(self):
        if self.end is False:
            self.end = True

    def run(self):
        global is_terminated
        global num_ch_query
        global plot_sync_count
        global plot_exec_count
        barrier_count = 0

        if self.group_query == True:
            idx = 0
        else:
            idx = self.client_id - 1


        if idx == 14:
            f_query = open(c_view_path, 'r')
            f_contents = f_query.read()
            self.cursor.execute(f_contents)
            f_query.close()

        times = 0

        '''
        if self.aide:
            self.cursor.execute('SELECT pg_enable_ndp();')
        '''

        with open(self.result_file, 'w') as f:
            epoch = time.perf_counter()

            while not self.end:
                while self.plot_worker:
                    if plot_exec_count.ok_to_go(barrier_count) != -1:
                        barrier_count += 1 
                        break
                    elif self.end:
                        return
                    else:
                        time.sleep(0.0001)

                start_time = time.perf_counter()

                query = self.make_query(idx)
                self.cursor.execute(query)

                self.cursor.fetchall()
                end_time = time.perf_counter()

                if self.plot_worker == True:
                    results = f'Q{idx+1} {times} {start_time:.3f} {end_time:.3f}\n'
                elif self.group_query == True:
                    results = "Q" + str(self.query_seq[idx]) + ' ' + str(times) + ' ' +  f'{(end_time - start_time):.3f}\n'
                else:
                    results = str(times) + ' ' +  f'{(end_time - start_time):.3f}\n'

                f.write(results)
                f.flush()
                    
                times += 1
                time.sleep(5)

                if self.group_query == True:
                    idx += 1
                    idx %= self.query_num

                if self.plot_worker:
                    plot_exec_count.exec_fin()

            self.db.commit()
            self.cursor.close()
            self.db.close()

    def make_query(self, idx):
        return self.ch_queries[idx] 

    def make_connector(self):
        return mysql.connector.connect(host=os.environ['DBHOST'],
                database=os.environ['DBDATABASE'],
                user=os.environ['DBUSER'],
                port=os.environ['DBPORT'],
                password=os.environ['DBPASSWORD'],
                unix_socket=os.environ['DBSOCKET'],
                autocommit=self.autocommit)

def give_stats(sent_query_amount, time_lapsed_in_secs, dir_path):
    with open(dir_path + "/ch_results_{}.txt".format(file_suffix), "w") as f:
        f.write("queries {} in {} seconds\n".format(sent_query_amount, time_lapsed_in_secs))
        f.write("QPH {}\n".format(3600.0 * sent_query_amount / time_lapsed_in_secs))

def get_curtime_in_seconds():
    return int(round(time.time()))

def terminate(dir_path):
    global is_terminated
    global sent_query_amount
    global start_time_in_secs

    end_time_in_secs = get_curtime_in_seconds()

    give_stats(sent_query_amount, end_time_in_secs - start_time_in_secs, dir_path)

    is_terminated = True

class GracefulKiller:
    kill_now = False
    def __init__(self, dir_path):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.dir_path = dir_path

    def exit_gracefully(self,signum, frame):
        global is_terminated
        self.kill_now = True
        print("got a kill signal")
        terminate(dir_path)

if __name__ == "__main__":
    long_thread_count = int(sys.argv[1])
    short_thread_count = int(sys.argv[2])
    aide_thread_count = 0
    plot_long_thread_count = 0
    plot_short_thread_count = 0

    is_group = False
    is_plot = False
    initial_sleep_in_mins=int(sys.argv[3])

    ISAIDE=sys.argv[4]
    ISGROUP=sys.argv[5]
    ISPLOT=sys.argv[6]
    
    query_num = int(sys.argv[7])

    aide_id = 1
    aide_num = 0

    global plot_sync_count
    global plot_exec_count
       
    dir_path = sys.argv[8]

    file_suffix=sys.argv[9] + "_"
    file_suffix+=datetime.now().strftime('%Y-%m-%d_%I:%M:%S_%p')

    short_thread_ids = [x for x in range(1, short_thread_count + 1)]
    long_thread_ids = [x for x in range(1, long_thread_count + 1)]

    if ISAIDE == "true":
        aide_thread_count = 1
        if ISGROUP == "true":
            is_group = True
            aide_num = query_num
        else:
            aide_id = query_num
 
    if ISPLOT == "true":
        is_plot = True
        is_group = False
        aide_thread_count = 1
        plot_long_thread_count = 1
        plot_short_thread_count = 1
        
        if query_num in short_thread_ids:
            short_thread_ids.remove(query_num)

        if query_num in long_thread_ids:
            long_thread_ids.remove(query_num)

        aide_id = query_num
        aide_num = 0

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    short_file_path = dir_path + "/ch_queries_{}_short_worker#{}.txt"
    long_file_path = dir_path + "/ch_queries_{}_long_worker#{}.txt"
    aide_file_path = dir_path + "/aide_ch_queries_{}_aide_worker#{}.txt"
    plot_short_file_path = dir_path + "/ch_queries_{}_plot_short_worker#{}.txt"
    plot_long_file_path = dir_path + "/ch_queries_{}_plot_long_worker#{}.txt"

    short_olap = []
    long_olap = []
    aide_olap = []
    plot_long_olap = []
    plot_short_olap = []


    plot_sync_count = aide_thread_count + plot_long_thread_count + plot_short_thread_count
    plot_exec_count = PlotWorkerBarrier(plot_sync_count)

    random.seed(RANDOM_SEED)

    if is_group == False and (query_num == 5 or query_num == 10 or query_num == 18):
        time.sleep(1)
    start_time_in_secs = get_curtime_in_seconds()

    # make olap client
    if is_plot:
        short_olap = [Client(client_id = i, autocommit=True, result_file=os.path.join(short_file_path.format(file_suffix, i)), aide=False, group=-1, group_query = False, plot = False) \
                for i in short_thread_ids]

        long_olap = [Client(client_id = i, autocommit=False, result_file=os.path.join(long_file_path.format(file_suffix, i)), aide=False, group=-1, group_query = False, plot = False) \
                for i in long_thread_ids]

        aide_olap = [Client(client_id = aide_id, autocommit=False, result_file=os.path.join(aide_file_path.format(file_suffix, aide_id)), aide=True, group=aide_num, group_query = False , plot = True) \
                for i in range(1, aide_thread_count + 1)]

        plot_short_olap = [Client(client_id = aide_id, autocommit=True, result_file=os.path.join(plot_short_file_path.format(file_suffix, aide_id)), aide=False, group=-1, group_query = False, plot = True) \
                for i in range(1, plot_short_thread_count + 1)]

        plot_long_olap = [Client(client_id = aide_id, autocommit=False, result_file=os.path.join(plot_long_file_path.format(file_suffix, aide_id)), aide=False, group=-1, group_query = False, plot = True) \
                for i in range(1, plot_long_thread_count + 1)]
    else:
        short_olap = [Client(client_id = i, autocommit=True, result_file=os.path.join(short_file_path.format(file_suffix, i)), aide=False, group=-1, group_query = False, plot = False) \
                for i in range(1, short_thread_count + 1)]

        long_olap = [Client(client_id = i, autocommit=False, result_file=os.path.join(long_file_path.format(file_suffix, i)), aide=False, group=-1, group_query = False, plot = False) \
                for i in range(1, long_thread_count + 1)]

        aide_olap = [Client(client_id = aide_id, autocommit=False, result_file=os.path.join(aide_file_path.format(file_suffix, aide_id)), aide=True, group=aide_num, group_query = is_group, plot = False) \
                for i in range(1, aide_thread_count + 1)]


    # run olap client
    for short_client in short_olap:
        short_client.start()

    for long_client in long_olap:
        long_client.start()

    if aide_thread_count == 1:
        aide_olap[0].start()

    if plot_short_thread_count == 1:
        plot_short_olap[0].start()

    if plot_long_thread_count == 1:
        plot_long_olap[0].start()

    killer = GracefulKiller(dir_path)

    while not killer.kill_now:
        time.sleep(10)

    # terminate olap client
    for short_client in short_olap:
        short_client.terminate()
    for long_client in long_olap:
        long_client.terminate()

    if aide_thread_count == 1:
        aide_olap[0].terminate()

    if plot_short_thread_count == 1:
        plot_short_olap[0].terminate()

    if plot_long_thread_count == 1:
        plot_long_olap[0].terminate()

    for short_client in short_olap:
        short_client.join()
    for long_client in long_olap:
        long_client.join()

    if aide_thread_count == 1:
        aide_olap[0].join()

    if plot_short_thread_count == 1:
        plot_short_olap[0].join()

    if plot_long_thread_count == 1:
        plot_long_olap[0].join()
