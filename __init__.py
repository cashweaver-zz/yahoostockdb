#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import logbook
import os
import sys
import configparser
import ta
import urllib2
import re
import time
#import sqlite3 as lite
from sqlite3 import dbapi2 as lite
import multiprocessing as mp
import ystockquote as ysq
import progressbar


config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.realpath(__file__))+"/config.ini")
symbol_list = config['DATABASE']['symbol_list_path']
db_path = config['DATABASE']['db_path']

# set up log
log = logbook.Logger('Logbook')

# get some dates
now = datetime.datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = now - datetime.timedelta(days=7)
last_week = last_week.strftime("%Y-%m-%d")
the_beginning = "1900-01-01"

# TODO move these to config file
# column names
cols_hist = "Date TEXT, " + \
        "Open REAL, " + \
        "High REAL, " + \
        "Low REAL, " + \
        "Close REAL, " + \
        "Volume INT, " + \
        "AdjClose REAL"

cols_ta = "Rsi14 REAL, " + \
        "Rsi20 REAL"

class Database(object):
    def __init__(self):
        pass

    def _get_all_table_names(self, cursor):
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        tables = cursor.fetchall()
        hist_tables = [re.sub('_HIST', '', table[0])
                        for table in tables
                        if not re.search('_TA', table[0])]
        return hist_tables

    def update_db(self):
        log.info("Checking for internet connection...")
        if self.internet_on():
            log.info("Connection found. Continuing...")
            producers = []
            try:
                log.info("Connecting to database...")
                con = lite.connect(config['DATABASE']['db_path'])
                cursor = con.cursor()
                with con:
                    log.info("Connection made. Updating...")
                    nprocs = mp.cpu_count()
                    log.info("%d cores detected." % nprocs)
                    log.info("(%d * 4) = %d processes will be created." % (nprocs, (nprocs * 4)))
                    nprocs = nprocs * 4

                    all_syms = self._get_all_table_names(cursor)
                    all_syms_and_lld = []
                    for s in all_syms:
                        all_syms_and_lld.append([s, self._get_latest_local_date(cursor, s)])
                    sub_syms = [all_syms_and_lld[i*len(all_syms_and_lld)//nprocs:(i+1)*len(all_syms_and_lld)//nprocs]
                                for i in range(nprocs)]

                    hist_data_q = mp.Queue()
                    pstatus = mp.Manager().dict()

                    pbar = progressbar.ProgressBar(
                        widgets=[
                            'Updating DB', ' ',
                            progressbar.Percentage(), ' ',
                            progressbar.Bar('=', '[', ']'), ' ',
                            progressbar.ETA()],
                            maxval=len(all_syms)
                        ).start()
                    pstatus[0] = 0
                    for i in range(nprocs):
                        pid = i + 1
                        child = mp.Process(
                            target=self._update_producer,
                            args=[sub_syms[i], hist_data_q, pstatus, pid])
                        producers.append(child)
                        child.start()

                    while any(p.is_alive() for p in producers):
                        try:
                            symbol, lrd, hist_data = hist_data_q.get(timeout=5)
                            pstatus[0] += 1
                            symbol = self.sterilize_symbol(symbol)
                            lrd = time.strptime(lrd, "%Y-%m-%d")

                            log.info("Updating: %s" % (symbol))
                            if not hist_data:
                                pass
                                log.info("Already up-to-date: %s" % (symbol))
                            elif lrd > last_week:
                                log.info("Updating: %s" % (symbol))
                                cursor.execute("DROP TABLE IF EXISTS %s_TA" % (symbol))
                                self._insert_tables(cursor, symbol, hist_data, ta.get_ta_data(hist_data))
                            else:
                                log.info("Remote data more than a week old. Dropping tables: %s" % (symbol))
                                self._drop_tables(cursor, symbol)
                        except:
                            pass
                        pbar.update(pstatus[0])

                    log.info("Update complete.")
            except lite.Error, e:
                log.critical("Error: %s: " % e.args[0])
                log.critical("Closing all processses...")
                for i, p in enumerate(producers):
                    p.join()
                    log.critical("Process %d closed." % i)
                sys.exit(1)
            except KeyboardInterrupt:
                log.critical("Database update interupted by keyboard")
                log.critical("Closing all processses...")
                for i, p in enumerate(producers):
                    p.join()
                    log.critical("Process %d closed." % i)
                log.warning("Your database may have become corrupted.")
                sys.exit(1)
        else:
            log.critical("Could not connect to google.com via [%s]. \
                         Conclusion:  You're not connected to the internet. \
                         Either that or google.com is down. \
                         2013-08-17 Never Forget." % config['DATABASE']['conn_test_ip'])

    def _update_producer(self, sub_syms, q, pstatus, pid):
        log.info("Process %02d starting" % (pid))
        for s in sub_syms:
            sym, lld = s
            try:
                hist_data = ysq.get_historical_prices(sym, lld, today)
                # remove header
                hist_data.pop(0)
                lrd = hist_data[0][0]
                # reverse order, so oldest are at top
                hist_data.reverse()
                # remove the data for date: lld
                hist_data.pop(0)
                q.put((sym, lrd, hist_data))
            except:
                log.info("Could not download data for %s" % s)
        log.info("Process %02d finished" % (pid))

    def init_db(self):
        print "init_db() deletes your existing database and downloads fresh data."
        print "Delete your existing database:"
        print "%s" % config['DATABASE']['db_path']
        uinput = raw_input("and continue? [y/N] ")
        if not uinput == 'y':
            print "Exiting."
            sys.exit(1)
        try:
            os.remove(config['DATABASE']['db_path'])
            log.info("Database deleted.")
        except:
            log.info("No database found by given name. Continuing.")

        log.info("Checking for internet connection...")
        if self.internet_on():
            log.info("Connection found. Continuing...")
            producers = []
            try:
                log.info("Connecting to database...")
                con = lite.connect(config['DATABASE']['db_path'])
                cursor = con.cursor()
                with con:
                    log.info("Connection made. Updating...")
                    nprocs = mp.cpu_count()
                    log.info("%d cores detected." % nprocs)
                    log.info("(%d * 4) = %d processes will be created." % (nprocs, (nprocs * 4)))
                    nprocs = nprocs * 4

                    all_syms = [i.strip() for i in open(symbol_list, 'r').readlines()]
                    sub_syms = [all_syms[i*len(all_syms)//nprocs:(i+1)*len(all_syms)//nprocs]
                                for i in range(nprocs)]

                    hist_data_q = mp.Queue()
                    pstatus = mp.Manager().dict()

                    pbar = progressbar.ProgressBar(
                        widgets=[
                            'Initializing DB', ' ',
                            progressbar.Percentage(), ' ',
                            progressbar.Bar('=', '[', ']'), ' ',
                            progressbar.ETA()],
                            maxval=len(all_syms)
                        ).start()
                    pstatus[0] = 0
                    for i in range(nprocs):
                        pid = i + 1
                        log.info("Creating process %d/%d" % (pid, nprocs))
                        child = mp.Process(
                            target=self._init_producer,
                            args=[sub_syms[i], hist_data_q, pstatus, pid])
                        producers.append(child)
                        child.start()

                    while any(p.is_alive() for p in producers):
                        try:
                            symbol, lrd, hist_data, ta_data = hist_data_q.get(timeout=5)
                            pstatus[0] += 1
                            symbol = self.sterilize_symbol(symbol)
                            log.info("Initializing: %s" % (symbol))
                            #self._drop_tables(cursor, symbol)
                            #log.info("Creating tables: %s" % (symbol))
                            self._create_tables(cursor, symbol)
                            #log.info("Inserting into tables: %s" % (symbol))
                            self._insert_tables(cursor, symbol, hist_data, ta_data)
                        except:
                            pass
                        pbar.update(pstatus[0])

                    log.info("Update complete.")
            except lite.Error, e:
                log.critical("Error: %s: " % e.args[0])
                log.critical("Closing all processses...")
                for i, p in enumerate(producers):
                    p.join()
                    log.critical("Process %d closed." % i)
                sys.exit(1)
            except KeyboardInterrupt:
                log.critical("Database update interupted by keyboard")
                log.critical("Closing all processses...")
                for i, p in enumerate(producers):
                    p.join()
                    log.critical("Process %d closed." % i)
                log.warning("Your database may have become corrupted.")
                sys.exit(1)
        else:
            log.critical("Could not connect to google.com via [%s]. \
                         Conclusion:  You're not connected to the internet. \
                         Either that or google.com is down. \
                         2013-08-17 Never Forget." % config['DATABASE']['conn_test_ip'])

    def _init_producer(self, sub_syms, q, pstatus, pid):
        log.info("Process %02d starting" % (pid))
        for s in sub_syms:
            try:
                hist_data = ysq.get_historical_prices(s, the_beginning, today)
                # remove header
                hist_data.pop(0)
                lrd = hist_data[0][0]
                # reverse order, so oldest are at top
                hist_data.reverse()
                ta_data = ta.get_ta_data(hist_data)
                q.put((s, lrd, hist_data, ta_data))
            except:
                log.info("Could not download data for %s" % s)
        log.info("Process %02d finished" % (pid))

    def _drop_tables(self, cursor, symbol):
        #log.info("Dropping tables: %s" % (symbol))
        cursor.execute("DROP TABLE IF EXISTS %s_HIST" % (symbol))
        cursor.execute("DROP TABLE IF EXISTS %s_TA" % (symbol))

    def _create_tables(self, cursor, symbol):
        #log.info("Creating tables: %s" % (symbol))
        cursor.execute("CREATE TABLE %s_HIST(%s)" % (symbol, cols_hist))
        cursor.execute("CREATE TABLE %s_TA(%s)" % (symbol, cols_ta))


    def _insert_tables(self, cursor, symbol, hist_data, ta_data):
        #log.info("Inserting into tables: %s" % (symbol))
        cursor.executemany("INSERT INTO %s_HIST VALUES(?, ?, ?, ?, ?, ?, ?)" % (symbol), (hist_data))
        cursor.executemany("INSERT INTO %s_TA VALUES(?, ?)" % (symbol), (ta_data))


    def _get_latest_local_date(self, cursor, symbol):
        symbol = symbol.upper()
        cursor.execute("SELECT * FROM %s_HIST WHERE oid = (SELECT MAX(oid) FROM %s_HIST)" % (symbol, symbol))
        return cursor.fetchone()[0]


    def _table_exists(self, cursor, t_name):
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'" % (t_name))
        return cursor.fetchone()


    def sterilize_symbol(self, symbol):
        return re.sub('[^a-zA-Z0-9]', '_', symbol.rstrip().upper())


    def internet_on(self):
        try:
            urllib2.urlopen('http://'+config['DATABASE']['conn_test_ip'], timeout=1)
            return True
        except urllib2.URLError: pass
        return False
