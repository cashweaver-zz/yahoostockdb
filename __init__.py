#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime, logbook, os, sys, configparser, math, time, collections, ta, urllib2, re
#import sqlite3 as lite
from sqlite3 import dbapi2 as lite
import multiprocessing as mp
import ystockquote as ysq
import progressbar


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


    def mp_update_db(self, symbol_list=symbol_list, db_path=db_path):
        log.info("Checking for internet connection...")
        if self.internet_on():
            log.info("Connection found. Continuing...")
            procs = []
            try:
                log.info("Updating database...")
                con = lite.connect(db_path)
                with con:

                    # Configuration Options
                    #======================
                    # Update interval in seconds
                    # Numbers below 1 may result in flickering
                    #pb_update_interval = 1
                    # Message to be printed once all processes finish
                    #end_msg =  'All processes complete!\n'
                    # Prefix for all process names. Be sure to include a trailing space for
                    # legibility
                    #pname_prefix = "Process "
                    # Header to be included above progress bars
                    #header = "Running processes"
                    # Number of processes
                    nprocs = mp.cpu_count()


                    # Create dummy list and data
                    #===========================
                    #nums = []
                    #nums.extend(range(1,49))
                    # Split nums, as equally as can be done, into a list of lists: sub_nums
                    #sub_nums = [nums[i*len(nums)//nprocs:(i+1)*len(nums)//nprocs] for i in range(nprocs)]
                    all_syms = [i.strip() for i in open(symbol_list, 'r').readlines()]
                    sub_syms = [all_syms[i*len(all_syms)//nprocs:(i+1)*len(all_syms)//nprocs] for i in range(nprocs)]


                    # Create overseer lists and dicts
                    #================================
                    # Dictionary to hold the current % of all processes,
                    # indexed by pid
                    pstatus = mp.Manager().dict()
                    # Create list to hold all processes.
                    procs = []
                    # Create list to hold all progress bars.
                    pbars = []


                    # Create the global progress bar
                    #===============================
                    # Define progress bar in the same way you would with the regular
                    # progressbar package.
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
                    #=======================================
                    for i in range(nprocs):
                        # Maximum value of the progress bar. Defined within the loop because
                        # this number should be equal to the operation count of the given
                        # process, which may not be equal for all processes.
                        pb_maxval = len(sub_nums[i])

                        # Initialize status of the current process to 0%
                        # pid = i+1 because 0 is reserved for the global progress bar
                        pstatus[i+1] = 0

                        # Define progress bar in the same way you would with the regular
                        # progressbar package
                        pbars.append(mpprogressbar.ProgressBar(
                            widgets=[
                                (config['DATABASE']['pb_proc_prefix'] + ' ' + str(i+1)), ' ',
                                mpprogressbar.Percentage(), ' ',
                                mpprogressbar.Bar('-', '[', ']'), ' ',
                                mpprogressbar.ETA()],
                                maxval=pb_maxval
                            ).start())


                        # Initialize the new process
                        #   target: function to be run by the process
                        #   args: list of arguments the function requires
                        child = mp.Process(
                            target=self.worker,
                            args=[db_path, sub_syms[i], i, status, con])

                        # Add child to procs so we can keep track of it
                        procs.append(child)

                        # Start the process
                        child.start()

                    # Prints progress bars while any one is still alive, then prints the end_msg
                    print_progressbars(
                        pstatus,
                        procs,
                        pbars,
                        config['DATABASE']['pb_header'],
                        config['DATABASE']['pb_refresh_interval'],
                        config['DATABASE']['pb_complete_msg']
                        )
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

    #args=[db_path, sub_syms[i], i, status, con])

    def do_stuff(sub_syms, pstatus, pid, pb_maxval):
        """
        Pretends to do work so we can see mpprogressbar in action.
        """
        for s in sub_syms:
            ##########
            # Do stuff
            self.update_symbol(con, s)
            # Done doing things
            ###################
            # update progress bar for this process
            pstatus[pid] += 1
            # update the global progress bar
            pstatus[0] += 1


        #def worker(self, db_path, syms, pid, status, con):
            """ The worker function, invoked in a process. 'syms' is a
                list of symbols to add to the database.
            """
            #count = len(syms)
            #for i, s in enumerate(syms):
                #status.put([pid, (i+1.0)/count])
                #self.update_symbol(con, s, db_path)


        #def print_progress(self, progress):
            #sys.stdout.write('\033[2J\033[H') #clear screen
            #pbar_width = int(config['DATABASE']['pbar_width'])
            #for proc_id, percent in progress.items():
                #bar = ('=' * int(percent * pbar_width)).ljust(pbar_width)
                #percent = int(percent * 100)
                #sys.stdout.write("%s [%s] %s%%\n" % ("Process %s" % proc_id, bar, percent))
            #sys.stdout.flush()



        # for passing data between update_yahoo_data and update_ta_data
        update_count = 0



    def update_db(self, symbol_list=symbol_list, db_path=db_path):
        log.info("Checking for internet connection...")
        if self.internet_on():
            log.info("Connection found. Continuing...")
            try:
                #log.info("Updating database...")
                con = lite.connect(":memory:")
                with con:
                    symbol_count = self.file_len(symbol_list)
                    pbar = progressbar.ProgressBar(
                        widgets=['Updating database: ',
                                progressbar.Bar('=', '[', ']'), ' ',
                                progressbar.Percentage(), ' ',
                                progressbar.ETA()],
                        maxval = symbol_count).start()
                    cur_sym = 0
                    i = 0
                    for symbol in open(symbol_list, 'r').readlines():
                        pbar.update(i+1)
                        i += 1
                        # sterilize symbol: change all special chars into '_'
                        symbol = self.sterilize_symbol(symbol)
                        self.update_symbol(con, symbol, db_path)
                        cur_sym += 1
            except lite.Error, e:
                log.critical("Error: %s: " % e.args[0])
                sys.exit(1)
            finally:
                if con:
                    con.close()
        else:
            log.critical("Could not connect to google.com via [%s]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget." % conn_test_ip)
            pass


    def update_symbol(self, con, symbol):
        db_path = config['DATABASE']['db_path']
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
        symbol = symbol.upper()
        # ensure connection to db
        cur = con.cursor()
        cur.execute("SELECT * FROM %s_HIST WHERE oid = (SELECT MAX(oid) FROM %s_HIST)" % (symbol, symbol))
        return cur.fetchone()[0]


    def get_latest_remote_date(self, symbol):
        # assumes that the most recent remote date will be in the last week
        try:
            hist_data = ysq.get_historical_prices(symbol, last_week, today)
            hist_data.pop(0)
            return hist_data[0][0]
        except:
            return ""


    def init_tables(self, con, symbol):
        symbol = self.sterilize_symbol(symbol)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS %s_HIST" % symbol)
        cur.execute("CREATE TABLE %s_HIST(%s)" % (symbol, cols_hist))
        cur.execute("DROP TABLE IF EXISTS %s_TA" % symbol)
        cur.execute("CREATE TABLE %s_TA(%s)" % (symbol, cols_ta))
        con.commit()


    def table_exists(self, con, t_name):
        """Check if table exists. Returns true/false"""
        cur = con.cursor()
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'" % (t_name))
        return cur.fetchone()


    def update_tables(self, con, symbol, sdate, edate):
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
        return re.sub('[^a-zA-Z0-9]', '_', symbol.rstrip().upper())


    def internet_on(self):
        try:
            urllib2.urlopen('http://'+conn_test_ip, timeout=1)
            return True
        except urllib2.URLError: pass
        return False


    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
