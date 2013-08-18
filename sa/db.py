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

config = configparser.ConfigParser()
config.read(os.getcwd()+"/config.ini")
symbol_list = config['DATABASE']['symbol_list']
db_path = config['DATABASE']['db_path']

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
    logging.info("Updating database...")
    lrd = get_latest_remote_date()
    for symbol in open(symbol_list, 'r').readlines():
        update_symbol(symbol.rstrip(), lrd, db_path)

def update_symbol(symbol, lrd, db_path):
    symbol = symbol.upper()
    # connect to db
    con = lite.connect(db_path)
    with con:
        if table_exists(con, symbol+"_HIST"):
            lld = get_latest_local_date(con, symbol)
            if lld == lrd:
                logging.info("%s\t| Up to date" % (symbol))
            else:
                update_tables(con, symbol, lld, today)
                logging.info("%s\t| Updated" % (symbol))
        else:
            logging.info("%s\t| No table found, creating and updating..." % (symbol))
            init_tables(con, symbol)
            update_tables(con, symbol, the_beginning, today)

def get_latest_local_date(con, symbol):
    symbol = symbol.upper()
    # ensure connection to db
    cur = con.cursor()
    cur.execute("SELECT * FROM %s_HIST WHERE oid = (SELECT MAX(oid) FROM %s_HIST)" % (symbol, symbol))
    return cur.fetchone()[0]

def get_latest_remote_date(symbol="GOOG"):
    # assumes that the most recent remote date will be in the last week
    hist_data = ysq.get_historical_prices(symbol, last_week, today)
    hist_data.pop(0)
    return hist_data[0][0]

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
