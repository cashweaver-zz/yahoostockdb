#!/usr/bin/python
# -*- coding: utf-8 -*-

import sa.mt_db as sadb
import logbook

log_handler = logbook.FileHandler('mt_sa.log')

with log_handler.applicationbound():
    sadb.mt_update_db(100)
