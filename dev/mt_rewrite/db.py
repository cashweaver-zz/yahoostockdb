import stockpredictor
import configparser, os, re, sys, math
import sqlite as lite

class db(stockpredictor):
    def __init__(self):
        pass

    def update(self, db_path):
        """
        Spawns threads and updates the database based on symbols pulled from the relpath specified in the config file.

        """
        nthreads = stockpredictor.config['Runtime']['nthreads']
        if self.internet_on():
            try:
                sql = MultiThreadOK(db_path)
                chunksize = int(math.ceil(self.get_symbol_count() / float(nthreads)))

                for i in range(nthreads):
                    syms = get_symbols_as_list()[chunksize * i:chunksize * (i + 1)]





                    lrd = get_latest_remote_date(symbol)
                    # check that Yahoo has data for given symbol
                    if not lrd == "":
                        if table_exists(con, symbol+"_HIST"):
                            lld = get_latest_local_date(con, symbol)
                            if lld == lrd:
                                log.info("%7s| Up to date" % (symbol))
                                pass
                            else:
                                update_tables(con, symbol, lld, today)
                                log.info("%7s| Updated" % (symbol))
                        else:
                            log.info("%7s| No table found:  Creating and updating..." % (symbol))
                            init_tables(con, symbol)
                            update_tables(con, symbol, the_beginning, today)
                    # otherwise the symbol doesn't exist in Yahoo's database
                    elif table_exists(con, symbol+"_HIST"):
                        log.info("%7s| Symbol doesn't exist on Yahoo. Table found. Dropping..." % (symbol))
                        cur = con.cursor()
                        cur.execute("DROP TABLE IF EXISTS %s_HIST" % symbol)
                        cur.execute("DROP TABLE IF EXISTS %s_TA" % symbol)
                    else:
                        log.info("%7s| Symbol doesn't exist on Yahoo. No table found. Skipping..." % (symbol))
                        pass







            except lite.Error, e:
                stockpredictor.log.critical("Error: %s: " % e.args[0])
                sys.exit(1)
        else:
            stockpredictor.log.critical("Could not connect to google.com via [%s]. Conclusion:  You're not connected to the internet. Either that or google.com is down. 2013-08-17 Never Forget." % conn_test_ip)
            sys.exit(1)

    def load_db(self):
        """
        Loads database into memory. One will be created if it doesn't already exist.

        @rtype:     object
        @return:    A connection to our database.
        """
        pass #TODO remove this


    def get_symbols_as_list(self):
        """
        Returns a list of symbols generated from the given relpath. Relpath is extended with os.getcwd.

        Symbols are automatically changed to ALL CAPS. All special characters are changed to underscores.

        @rtype:     list
        @return:    A list of symbols generated from the given relpath.
        """
        relpath = stockpredictor.config['Database']['symbol_relpath']
        if relpath[0] == '/':
            fpath = os.getcwd()+relpath
        else:
            fpath = os.getcwd()+'/'+relpath

        try:
            symbols = open(fpath, 'r').readlines().splitlines().upper()
            return re.sub('[^a-zA-Z0-9]', '_', symbols)
        except IOError:
            stockpredictor.log.critical("Symbol file [%s] could not be opened." % fpath)
            sys.exit(1)

    def internet_on(self):
        """
        """
        stockpredictor.log.info("Checking for internet connection...")
        try:
            response = urllib2.urlopen('http://'+conn_test_ip, timeout=1)
            stockpredictor.log.info("Connection found. Continuing...")
            return True
        except urllib2.URLError as err: pass
        return False

    def get_symbol_count(self):
        """
        """
        relpath = stockpredictor.config['Database']['symbol_relpath']
        if relpath[0] == '/':
            fpath = os.getcwd()+relpath
        else:
            fpath = os.getcwd()+'/'+relpath

        with open(fpath) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

class MultiThreadOK(Thread):
    def __init__(self, db_path):
        super(MultiThreadOK, self).__init__()
        self.db_path=db_path
        self.reqs=Queue()
        self.start()
    def run(self):
        cnx = lite.Connection(self.db_path)
        cursor = cnx.cursor()
        while True:
            req, arg, res = self.reqs.get()
            if req=='--close--': break
            cursor.execute(req, arg)
            if res:
                for rec in cursor:
                    res.put(rec)
                res.put('--no more--')
        cnx.close()
    def execute(self, req, arg=None, res=None):
        self.reqs.put((req, arg or tuple(), res))
    def select(self, req, arg=None):
        res=Queue()
        self.execute(req, arg, res)
        while True:
            rec=res.get()
            if rec=='--no more--': break
            yield rec
    def close(self):
        self.execute('--close--')
