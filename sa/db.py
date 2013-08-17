#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import ystockquote as ysq
import datetime
import logging
import os
import sys

# -----------------------------------------------------------------------------
# Feel free to change these variables
default_db_path = os.getcwd() + "/sa.sql"
default_symbol_list = os.getcwd() + "/symbol_lists/example.txt"

## Stop changing things below this line.
# -----------------------------------------------------------------------------

# set up logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# get some dates
now = datetime.datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = now - datetime.timedelta(days=7)
last_week = last_week.strftime("%Y-%m-%d")
the_beginning = "1900-01-01"

def update_symbol(symbol, lrd, db_path):
    """Update a single symbol"""
    symbol = symbol.upper()
    # connect to db
    con = lite.connect(db_path)
    with con:
        if table_exists(con, symbol):
            lld = get_latest_local_date(con, symbol)
            if lld == lrd:
                logging.info("%s\t| Up to date" % (symbol))
            else:
                insert_yahoo_data(con, symbol, lld, today, True)
                logging.info("%s\t| Updated" % (symbol))
        else:
            logging.info("%s\t| No table found, creating and updating..." % (symbol))
            init_symbol(con, symbol, db_path)

def table_exists(con, t_name):
    """Check if table exists. Returns true/false"""
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'" % (t_name))
    return cur.fetchone()

def update_db(symbol_list=default_symbol_list, db_path=default_db_path):
    """Loops over symbol_list and calls update_symbol() for each"""
    logging.info("Updating database...")
    lrd = get_latest_remote_date()
    for symbol in open(symbol_list, 'r').readlines():
        update_symbol(symbol.rstrip(), lrd, db_path)

def init_db(symbol_list=default_symbol_list, db_path=default_db_path):
    """Loops over symbol_list and calls init_symbol() for each"""
    logging.info("Initializing database...")
    con = lite.connect(db_path)
    with con:
        for symbol in open(symbol_list, 'r').readlines():
            symbol = symbol.rstrip()
            init_symbol(con, symbol, db_path)
            logging.info("%s\t| initialized" % (symbol))

def init_symbol(con, symbol, db_path):
    """CREATE TABLE IF NOT EXISTS, then fill it with fresh Yahoo data"""
    symbol = symbol.upper()
    with con:
        insert_yahoo_data(con, symbol, the_beginning, today)

def insert_yahoo_data(con, symbol, sdate, edate, update=False):
    """Insert yahoo data into table"""
    cur = con.cursor()
    # TODO: sterilize sdate and edate
    hist_data = ysq.get_historical_prices(symbol, sdate, edate)
    # remove the headers
    hist_data.pop(0)
    # reverse order is much simpler for generating the technical indicators
    hist_data.reverse()
    if update:
        # hist_data includes data from lld, which we already have
        # it's the oldest data, so we can pop it off the top
        hist_data.pop(0)
    cur.executemany("INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?, ?)" % (symbol), (hist_data))

def get_latest_local_date(con, symbol, db_path=default_db_path):
    """Returns the Date field of the 'last' row for a given symbol"""
    symbol = symbol.upper()
    # connect to db
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM %s WHERE oid = (SELECT MAX(oid) FROM %s)" % (symbol, symbol))
        return cur.fetchone()[0]

def get_latest_remote_date(symbol="GOOG"):
    """Checks yahoo for the latest data date"""
    # assumes that the most recent remote date will be in the last week
    hist_data = ysq.get_historical_prices(symbol, last_week, today)
    hist_data.pop(0)
    return hist_data[0][0]
