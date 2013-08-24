#!/usr/bin/python
# -*- coding: utf-8 -*-

import talib
import numpy as np

def get_ta_data(hist_data):
    np_hist_data = np.array(hist_data)
    ac = np_hist_data[:,6].astype(np.float)
    rsi_14 = get_rsi(ac, 14)
    rsi_20 = get_rsi(ac, 20)
    return np.column_stack((
        rsi_14,
        rsi_20
        ))

def get_rsi(ac, timeperiod=14):
    return np.nan_to_num(talib.RSI(ac, timeperiod))
