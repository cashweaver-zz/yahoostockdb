#!/usr/bin/python
# -*- coding: utf-8 -*-

import psp.mp_db as sadb
import logbook, configparser, os

config = configparser.ConfigParser()
config.read(os.getcwd()+"/config.ini")

log_handler = logbook.FileHandler(config['DEBUG']['log_fpath'])

with log_handler.applicationbound():
    sadb.mp_update_db()
