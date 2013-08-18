#!/usr/bin/python
# -*- coding: utf-8 -*-

import talib
import numpy as np

def get_ta_data(con, symbol):
    rsi_14 = get_rsi(con, symbol, 14)
    rsi_20 = get_rsi(con, symbol, 20)
    #return np.vstack((rsi_1, rsi_2))
    return np.column_stack((
        rsi_14,
        rsi_20
        ))

def get_rsi(con, symbol, timeperiod=14):
    ac = get_adjclose_data(con, symbol)
    return np.nan_to_num(talib.RSI(ac, timeperiod))

def get_adjclose_data(con, symbol):
    cur = con.cursor()
    return np.array(cur.execute("SELECT AdjClose FROM %s_HIST" % (symbol)).fetchall()).flatten()
