#!/usr/bin/python
# -*- coding: utf-8 -*-

import sa.mp_db as sadb
import logbook

log_handler = logbook.FileHandler('mp_sa.log')

with log_handler.applicationbound():
    sadb.mp_update_db()
