#!/usr/bin/python
# -*- coding: utf-8 -*-

import sa.db as sadb
import logbook

#log = logbook.Logger('Logbook')
log_handler = logbook.FileHandler('sa.log')

with log_handler.applicationbound():
    sadb.update_db()
