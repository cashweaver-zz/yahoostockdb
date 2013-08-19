#!/usr/bin/python
# -*- coding: utf-8 -*-

#import sqlite3 as lite
from sqlite3 import dbapi2 as lite
import datetime, logbook, os, sys, configparser, math, threading, sqlalchemy
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

def mt_update_db(nthreads, symbol_list=symbol_list, db_path=db_path):
    log.info("Checking for internet connection...")
    if sadb.internet_on():
        log.info("Connection found. Continuing...")
        #log.info("Updating database...")
        def worker(syms):
            """ The worker function, invoked in a process. 'syms' is a
                list of symbols to add to the database.
            """
            try:
                #con = lite.connect(db_path, check_same_thread=False)
                con = lite.connect(":memory:", check_same_thread=False)
                with con:
                    for s in syms:
                        sadb.update_symbol(con, s, db_path)
                        con.commit()
            except lite.Error, e:
                log.critical("Error: %s: " % e.args[0])
                sys.exit(1)
            finally:
                if con:
                    con.close()

        chunksize = int(math.ceil(sadb.file_len(symbol_list) / float(nthreads)))
        #threads = []
        all_syms = [i.strip() for i in open(symbol_list, 'r').readlines()]

        p = sqlalchemy.pool.SingletonThreadPool(lambda: lite.connect(db_path))

        for i in range(nthreads):
            syms = all_syms[chunksize * i:chunksize * (i + 1)]
            t = threading.Thread(
                target=worker,
                args=[syms])
            #t.daemon = True
            t.start()
            #p = multiprocessing.Process(
                #target=worker,
                #args=[syms])
            #procs.append(p)
            #p.start()
    else:
        log.critical("Could not connect to google.com via [%s]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget." % conn_test_ip)
        pass
