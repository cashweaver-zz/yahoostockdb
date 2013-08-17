#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import ystockquote as ysq
import datetime
import logging
import os
import sys

# set up logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# you should change this to your own default db
default_db_path = os.getcwd() + "/../sa.sql"

# get today's date in format:  2000-02-09
now = datetime.datetime.now()
today = now.strftime("%Y-%m-%d")

def update_symbol(symbol, db_path=default_db_path):
    symbol = symbol.upper()
    # connect to db
    con = lite.connect(db_path)
    with con:
        cur = con.cursor()
        # get the last row from table
        # this query doesn't exactly get the last row, but we shouldn't be
        # deleting any rows, so we should be fine
        cur.execute("SELECT * FROM %s WHERE oid = (SELECT MAX(oid) FROM %s)" % (symbol, symbol))
        data = cur.fetchone()
        sdate = data[0]
        # don't update if we're already up to date
        if not today == sdate:
            hist_data = ysq.get_historical_prices(symbol, sdate, today)
            # remove the headers
            hist_data.pop(0)
            # reverse order is much simpler for generating the technical indicators
            hist_data.reverse()
            cur.executemany("INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?, ?)" % (symbol), (hist_data))
            logging.info("%s has been updated." % symbol)
        else:
            logging.info("%s is already up to date." % symbol)


def add_symbol(symbol, sdate='1900-01-01', edate=today, db_path=default_db_path):
    # TODO: add sterilization for
    #   sdate
    #   edate
    symbol = symbol.upper()
    # connect to db
    con = lite.connect(db_path)
    with con:
        cur = con.cursor()
        # TODO change to only extend existing tables, not overwrite them
        cur.execute("DROP TABLE IF EXISTS %s" % (symbol))
        cur.execute("CREATE TABLE %s(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume INT, AdjClose REAL)" % (symbol))
        # TODO: sterilize sdate and edate
        hist_data = ysq.get_historical_prices(symbol, sdate, edate)
        # remove the headers
        hist_data.pop(0)
        # reverse order is much simpler for generating the technical indicators
        hist_data.reverse()
        cur.executemany("INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?, ?)" % (symbol), (hist_data))
