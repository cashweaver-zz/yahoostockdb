#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import datetime, logbook, os, sys, configparser, multiprocessing, math, collections, time
import db as sadb

config = configparser.ConfigParser()
config.read(os.getcwd()+"/config.ini")
symbol_list = config['DATABASE']['symbol_list']
db_path = config['DATABASE']['db_path']
conn_test_ip = config['DATABASE']['conn_test_ip']

# set up log
log = logbook.Logger('Logbook')

# get some dates
now = datetime.datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = now - datetime.timedelta(days=7)
last_week = last_week.strftime("%Y-%m-%d")
the_beginning = "1900-01-01"

def mp_update_db(symbol_list=symbol_list, db_path=db_path):
    log.info("Checking for internet connection...")
    if sadb.internet_on():
        log.info("Connection found. Continuing...")
        procs = []
        try:
            log.info("Updating database...")
            con = lite.connect(db_path)
            with con:
                nprocs = multiprocessing.cpu_count()
                chunksize = int(math.ceil(sadb.file_len(symbol_list) / float(nprocs)))
                status = multiprocessing.Queue()
                progress = collections.OrderedDict()
                all_syms = [i.strip() for i in open(symbol_list, 'r').readlines()]

                for i in range(nprocs):
                    syms = all_syms[chunksize * i:chunksize * (i + 1)]
                    child = multiprocessing.Process(target=worker, args=[db_path, syms, i, status, con])
                    progress[i] = 0.0
                    child.start()
                    procs.append(child)
                    child.join()

                pbar_refresh_interval = float(config['DATABASE']['pbar_refresh_interval'])
                while any(i.is_alive() for i in procs):
                    time.sleep(pbar_refresh_interval)
                    while not status.empty():
                        proc_id, percent = status.get()
                        progress[proc_id] = percent
                        #print_progress(progress)
                print 'all downloads complete'

        except lite.Error, e:
            log.critical("Error: %s: " % e.args[0])
            log.critical("Closing all processses...")
            for i, p in enumerate(procs):
                p.join()
                log.critical("Process %d closed." % i)

            sys.exit(1)
        finally:
            pass
            #if con:
                #con.close()
    else:
        log.critical("Could not connect to google.com via [%s]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget." % conn_test_ip)
        pass

def worker(db_path, syms, proc_id, status, con):
    """ The worker function, invoked in a process. 'syms' is a
        list of symbols to add to the database.
    """
    #con = lite.connect(db_path, check_same_thread=True)
    #con = lite.connect(db_path)
    count = len(syms)
    for i, s in enumerate(syms):
        status.put([proc_id, (i+1.0)/count])
        sadb.update_symbol(con, s, db_path)

def print_progress(progress):
    sys.stdout.write('\033[2J\033[H') #clear screen
    pbar_width = int(config['DATABASE']['pbar_width'])
    for proc_id, percent in progress.items():
        bar = ('=' * int(percent * pbar_width)).ljust(pbar_width)
        percent = int(percent * 100)
        sys.stdout.write("%s [%s] %s%%\n" % ("Process %s" % proc_id, bar, percent))
    sys.stdout.flush()
