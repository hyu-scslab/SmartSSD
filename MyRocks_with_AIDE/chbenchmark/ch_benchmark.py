#!/usr/bin/python3

import sys
import random
import atexit
import signal
import subprocess
import os

from threading import Thread
from time import sleep
import time

current_workdir = os.getcwd()
ch_query_file_path = current_workdir + "/ch-queries/q"

sent_query_amount = 0
is_terminated = False
file_suffix="0"

RANDOM_SEED = 123
num_ch_query = 22

ch_queries_path = [ch_query_file_path + str(x) + ".sql" for x in range(1,23)]
ch_queries = []

def start_ch_thread(start_index):
    global sent_query_amount
    global ch_queries
    global is_terminated

    size = len(ch_queries)

    cur_index = start_index
    while not is_terminated:
        return_code = send_query(ch_queries[cur_index], cur_index)
        # if there was an error, we will retry the same query
        if return_code != 0:
            continue
        sent_query_amount += 1

        cur_index += 1
        cur_index %= size

def send_query(query,cur_index):
    global coord_ip
    mysql = '../../myrocks-vanilla/myrocks-smartssd/inst/bin/mysql'
    pg = [mysql, '-h', coord_ip, '-u', 'root', '-pdaktopia', '-e', query, 'chbench']

    start_time = int(round(time.time() * 1000))
    return_code = subprocess.call(pg)
    end_time = int(round(time.time() * 1000))

    with open("results/ch_queries_{}.txt".format(file_suffix), "a") as f:
        f.write("{} finished in {} milliseconds\n".format(cur_index+1, end_time - start_time))

    return return_code

def give_stats(sent_query_amount, time_lapsed_in_secs):
    with open("results/ch_results_{}.txt".format(file_suffix), "w") as f:
        f.write("queries {} in {} seconds\n".format(sent_query_amount, time_lapsed_in_secs))
        f.write("QPH {}\n".format(3600.0 * sent_query_amount / time_lapsed_in_secs))

def get_curtime_in_seconds():
    return int(round(time.time()))


def terminate():
    global is_terminated
    global sent_query_amount
    global start_time_in_secs

    end_time_in_secs = get_curtime_in_seconds()

    give_stats(sent_query_amount, end_time_in_secs - start_time_in_secs)

    is_terminated = True

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        global is_terminated
        self.kill_now = True
        print("got a kill signal")
        terminate()

if __name__ == "__main__":
    thread_count = int(sys.argv[1])
    coord_ip = sys.argv[2]
    initial_sleep_in_mins=int(sys.argv[3])
    file_suffix=sys.argv[4]

    random.seed(RANDOM_SEED)

    start_indexes = list(range(0, 22))
    random.shuffle(start_indexes)

    for query_path in ch_queries_path:
        f_query = open(query_path, 'r')
        f_contents = f_query.read()
        ch_queries.append(f_contents)
        f_query.close()

    jobs = [
        Thread(target = start_ch_thread, args=(start_indexes[i % len(start_indexes)], ))
        for i in range(0, thread_count)]

    sleep(initial_sleep_in_mins * 60)

    start_time_in_secs = get_curtime_in_seconds()
    for j in jobs:
        j.start()

    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(10)
