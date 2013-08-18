#!/usr/bin/python
# -*- coding: utf-8 -*-

import ta
import sqlite3 as lite
import ystockquote as ysq
import datetime
import logging
import os
import sys
import configparser
import urllib2

config = configparser.ConfigParser()
config.read(os.getcwd()+"/config.ini")
symbol_list = config['DATABASE']['symbol_list']
db_path = config['DATABASE']['db_path']
conn_test_ip = config['DATABASE']['conn_test_ip']

# set up logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

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

# for passing data between update_yahoo_data and update_ta_data
update_count = 0

def update_db(symbol_list=symbol_list, db_path=db_path):
    logging.info("Checking for internet connection...")
    if internet_on():
        logging.info("Connection found. Continuing...")
        try:
            logging.info("Updating database...")
            logging.info("%7s| Status" % "Symbol")
            logging.info("======================")
            con = lite.connect(db_path)
            with con:
                for symbol in open(symbol_list, 'r').readlines():
                    update_symbol(con, symbol.rstrip().upper(), db_path)
        except lite.Error, e:
            logging.critical("Error: %s: " % e.args[0])
            sys.exit(1)
        finally:
            if con:
                con.close()
    else:
        logging.critical("Could not connect to google.com via [74.125.113.99]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget.")

def update_symbol(con, symbol, db_path):
    lrd = get_latest_remote_date(symbol)
    # check that Yahoo has data for given symbol
    if not lrd == "":
        if table_exists(con, symbol+"_HIST"):
            lld = get_latest_local_date(con, symbol)
            if lld == lrd:
                logging.info("%9s| Up to date" % (symbol))
            else:
                update_tables(con, symbol, lld, today)
                logging.info("%9s| Updated" % (symbol))
        else:
            logging.info("%9s| No table found:  Creating and updating..." % (symbol))
            init_tables(con, symbol)
            update_tables(con, symbol, the_beginning, today)
    # otherwise the symbol doesn't exist in Yahoo's database
    else:
        logging.debug("%8s| No table found:  Symbol doesn't exist on Yahoo. Skipping..." % (symbol))


def get_latest_local_date(con, symbol):
    symbol = symbol.upper()
    # ensure connection to db
    cur = con.cursor()
    cur.execute("SELECT * FROM %s_HIST WHERE oid = (SELECT MAX(oid) FROM %s_HIST)" % (symbol, symbol))
    return cur.fetchone()[0]

def get_latest_remote_date(symbol):
    # assumes that the most recent remote date will be in the last week
    try:
        hist_data = ysq.get_historical_prices(symbol, last_week, today)
        hist_data.pop(0)
        return hist_data[0][0]
    except:
        return ""

def init_tables(con, symbol):
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS %s_HIST" % symbol)
    cur.execute("CREATE TABLE %s_HIST(%s)" % (symbol, cols_hist))
    cur.execute("DROP TABLE IF EXISTS %s_TA" % symbol)
    cur.execute("CREATE TABLE %s_TA(%s)" % (symbol, cols_ta))

def table_exists(con, t_name):
    """Check if table exists. Returns true/false"""
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'" % (t_name))
    return cur.fetchone()

def update_tables(con, symbol, sdate, edate):
    update_count = 0
    cur = con.cursor()
    hist_data = ysq.get_historical_prices(symbol, sdate, edate)
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

    ta_data = ta.get_ta_data(con, symbol).tolist()
    if not sdate == the_beginning:
        ta_data = ta_data[-update_count:]
    cur.executemany("INSERT INTO %s_TA VALUES(?, ?)" % (symbol), (ta_data))

def internet_on():
    try:
        response = urllib2.urlopen('http://'+conn_test_ip, timeout=1)
        return True
    except urllib2.URLError as err: pass
    return False
