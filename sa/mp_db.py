#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import datetime, logbook, os, sys, configparser, multiprocessing, math
#import progressbar
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
        try:
            log.info("Updating database...")
            con = lite.connect(db_path)
            with con:
                def worker(syms):
                    """ The worker function, invoked in a process. 'syms' is a
                        list of symbols to add to the database.
                    """
                    for s in syms:
                        sadb.update_symbol(con, s, db_path)

                nprocs = multiprocessing.cpu_count()
                chunksize = int(math.ceil(sadb.file_len(symbol_list) / float(nprocs)))
                procs = []

                all_syms = [i.strip() for i in open(symbol_list, 'r').readlines()]

                for i in range(nprocs):
                    syms = all_syms[chunksize * i:chunksize * (i + 1)]
                    p = multiprocessing.Process(
                        target=worker,
                        args=[syms])
                    procs.append(p)
                    p.start()

        except lite.Error, e:
            log.critical("Error: %s: " % e.args[0])
            sys.exit(1)
        finally:
            if con:
                con.close()
    else:
        log.critical("Could not connect to google.com via [%s]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget." % conn_test_ip)
        pass
