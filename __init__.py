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
import mpprogressbar


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
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        pass


    def update_db(self):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        """
        Updates the database using multiprocessing, splitting all symbols
        defined in the symbol list over [multiprocessing.cpu_count()] processes.
        """

        log.info("Checking for internet connection...")
        if self.internet_on():
            log.info("Connection found. Continuing...")
            procs = []
            try:
                log.info("Updating database...")
                con = lite.connect(config['DATABASE']['db_path'])
                with con:
                    # Get the number of processors on our system
                    nprocs = mp.cpu_count()
                    log.info("%d cores detected." % nprocs)

                    # Pull all symbols from given symbol_list_path
                    sym_path = config['DATABASE']['symbol_list_path']
                    all_syms = [i.strip() for i in open(sym_path, 'r').readlines()]
                    # Evenly distribute all symbols over nproc lists
                    sub_syms = [all_syms[i*len(all_syms)//nprocs:(i+1)*len(all_syms)//nprocs] for i in range(nprocs)]
                    log.info("%d symbols found." % len(all_syms))

                    # Dictionary to hold the current % of all processes,
                    #   indexed by pid
                    pstatus = mp.Manager().dict()
                    # Create list to hold all processes.
                    procs = []
                    # Create list to hold all progress bars.
                    pbars = []


                    # Create the global progress bar
                    #   NOTE: maxval must equal the sum of maxvals for all processes
                    pbars.append(mpprogressbar.ProgressBar(
                        widgets=[
                            'All Procs', ' ',
                            mpprogressbar.Percentage(), ' ',
                            mpprogressbar.Bar('#', '[', ']'), ' ',
                            mpprogressbar.ETA()],
                            maxval=len(all_syms)
                        ).start())

                    # Initialize status of the global process to 0%
                    pstatus[0] = 0

                    # Create a progress bar for each process
                    for i in range(nprocs):
                        # pid = i+1 because 0 is reserved for the global progress bar
                        pid = i + 1
                        # Initialize status of the current process to 0%
                        pstatus[pid] = 0
                        # Create db connection
                        #con = lite.connect(config['DATABASE']['db_path'])

                        # Define progress bar for each process
                        pbars.append(mpprogressbar.ProgressBar(
                            widgets=[
                                ('Process ' + str(pid)), ' ',
                                mpprogressbar.Percentage(), ' ',
                                mpprogressbar.Bar('-', '[', ']'), ' ',
                                mpprogressbar.ETA()],
                                maxval=len(sub_syms[i])
                            ).start())


                        log.info("Creating process %d/%d" % (pid, nprocs))
                        # Initialize the new process
                        #   target: function to be run by the process
                        #   args: list of arguments the function requires
                        child = mp.Process(
                            target=self.worker,
                            args=[sub_syms[i], pstatus, pid, con])

                        # Add child to procs so we can keep track of it
                        procs.append(child)

                        log.info("Starting process %d/%d" % (pid, nprocs))
                        # Start the process
                        child.start()

                    # Prints progress bars while any one is still alive
                    self.print_progressbars(
                        pstatus,
                        procs,
                        pbars)
            except lite.Error, e:
                log.critical("Error: %s: " % e.args[0])
                log.critical("Closing all processses...")
                for i, p in enumerate(procs):
                    p.join()
                    log.critical("Process %d closed." % i)
                sys.exit(1)
            except KeyboardInterrupt:
                log.critical("Database update interupted by keyboard")
                log.critical("Closing all processses...")
                for i, p in enumerate(procs):
                    p.join()
                    log.critical("Process %d closed." % i)
                log.warning("Your database may have become corrupted.")
                sys.exit(1)
            finally:
                log.warning("Open connection found. Closing...")
                if con:
                    con.close()
        else:
            log.critical("Could not connect to google.com via [%s]. \
                         Conclusion:  You're not connected to the internet. \
                         Either that or google.com is down. \
                         2013-08-17 Never Forget." % config['DATABASE']['conn_test_ip'])

    def worker(self, sub_syms, pstatus, pid, con):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        """
        Each process invoked by update_db() is set to this function.
        """
        for s in sub_syms:
            self.update_symbol(con, s)
            # update progress bar for this process
            pstatus[pid] += 1
            # update the global progress bar
            pstatus[0] += 1

    def print_progressbars(self, pstatus, procs, pbars):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        """
        Print all progress bars while any one is still running.
        """
        header_msg = config['DATABASE']['pb_header']
        pb_refresh_interval = float(config['DATABASE']['pb_refresh_interval'])
        # Immediately clear the screen and print our header.
        # Without this, if the pb_update_interval is relatively long, the script
        # will appear to 'hang', even though it is running properly.
        #sys.stderr.write('\033[2J\033[H') # clear screen
        print header_msg

        # Continually update all progress bars so long as any one is still alive
        while any(p.is_alive() for p in procs):
            time.sleep(pb_refresh_interval)
            #sys.stderr.write('\033[2J\033[H') # clear screen
            #sys.stderr.write('\033[2J\033[H') # clear screen
            sys.stderr.write('\n');
            print header_msg
            # progressbar handles the actual printing of progress bars
            for i, pbar in enumerate(pbars):
                # progressbar prints the bar each time update() is called
                pbar.update(pstatus[i])
        print config['DATABASE']['pb_complete_msg']


    def update_symbol(self, con, symbol):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        lrd = self.get_latest_remote_date(symbol)
        # check that Yahoo has data for given symbol
        if not lrd == "":
            if self.table_exists(con, symbol+"_HIST"):
                lld = self.get_latest_local_date(con, symbol)
                if lld == lrd:
                    log.info("%7s| Up to date" % (symbol))
                    pass
                else:
                    self.update_tables(con, symbol, lld, today)
                    log.info("%7s| Updated" % (symbol))
            else:
                log.info("%7s| No table found:  Creating and updating..." % (symbol))
                self.init_tables(con, symbol)
                self.update_tables(con, symbol, the_beginning, today)
        # otherwise the symbol doesn't exist in Yahoo's database
        elif self.table_exists(con, symbol+"_HIST"):
            log.info("%7s| Symbol doesn't exist on Yahoo. Table found. Dropping..." % (symbol))
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS %s_HIST" % symbol)
            cur.execute("DROP TABLE IF EXISTS %s_TA" % symbol)
        else:
            log.info("%7s| Symbol doesn't exist on Yahoo. No table found. Skipping..." % (symbol))
            pass


    def get_latest_local_date(self, con, symbol):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        symbol = symbol.upper()
        # ensure connection to db
        cur = con.cursor()
        cur.execute("SELECT * FROM %s_HIST WHERE oid = (SELECT MAX(oid) FROM %s_HIST)" % (symbol, symbol))
        return cur.fetchone()[0]


    def get_latest_remote_date(self, symbol):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        # assumes that the most recent remote date will be in the last week
        try:
            hist_data = ysq.get_historical_prices(symbol, last_week, today)
            hist_data.pop(0)
            return hist_data[0][0]
        except:
            return ""


    def init_tables(self, con, symbol):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        symbol = self.sterilize_symbol(symbol)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS %s_HIST" % symbol)
        cur.execute("CREATE TABLE %s_HIST(%s)" % (symbol, cols_hist))
        cur.execute("DROP TABLE IF EXISTS %s_TA" % symbol)
        cur.execute("CREATE TABLE %s_TA(%s)" % (symbol, cols_ta))
        con.commit()


    def table_exists(self, con, t_name):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        """
        Check if table exists.

        @rtype: bool
        @return: True, if the table exists, else false.
        """
        cur = con.cursor()
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'" % (t_name))
        return cur.fetchone()


    def update_tables(self, con, symbol, sdate, edate):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        update_count = 0
        cur = con.cursor()
        hist_data = ysq.get_historical_prices(symbol, sdate, edate)
        symbol = self.sterilize_symbol(symbol)
        # remove the headers
        hist_data.pop(0)
        # reverse order is much simpler for generating the technical indicators
        hist_data.reverse()
        if not sdate == the_beginning:
            # hist_data includes data from lld, which we already have
            # it's the oldest data, so we can pop it off the top
            hist_data.pop(0)
            update_count = len(hist_data)
        cur.executemany("INSERT INTO %s_HIST VALUES(?, ?, ?, ?, ?, ?, ?)" % (symbol), (hist_data))
        con.commit()

        ta_data = ta.get_ta_data(con, symbol).tolist()
        if not sdate == the_beginning:
            ta_data = ta_data[-update_count:]
        cur.executemany("INSERT INTO %s_TA VALUES(?, ?)" % (symbol), (ta_data))
        con.commit()


    def sterilize_symbol(self, symbol):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        return re.sub('[^a-zA-Z0-9]', '_', symbol.rstrip().upper())


    def internet_on(self):
        """
        Desc

        @type: number
        @param: the_parameter

        @rtype: number
        @return: A_number
        """
        try:
            urllib2.urlopen('http://'+config['DATABASE']['conn_test_ip'], timeout=1)
            return True
        except urllib2.URLError: pass
        return False


    def file_len(self, fname):
        """
        Returns the length of a file

        @type: string
        @param: Name of the file to be checked.

        @rtype: number
        @return: A_number
        """
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
