stock_analyzer
=======

Python based stock analysis

## Installing

This guide was written for Ubuntu 12.04. Your mileage may vary.

### Required Libraries

All the commands line by line:

```bash
    sudo apt-get -y install python-dev python-pip
    sudo pip install cython
    sudo pip install numpy
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar -xzf ta-lib-0.4.0-src.tar.gz
    cd ta-lib
    ./configure --prefix=/usr
    make
    sudo make install
    sudo pip install TA-lib
    sudo pip install ystockquote
```

All the commands as one line:
```bash
    sudo apt-get -y install python-dev python-pip; sudo pip install cython; sudo pip install numpy; wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz; tar -xzf ta-lib-0.4.0-src.tar.gz; cd ta-lib; ./configure --prefix=/usr; make; sudo make install; sudo pip install TA-lib; sudo pip install ystockquote;
```

## Setting up

### Sql file

You need a database to work with. While in the root directory of this repository, run the following command:
    sqlite3 sa.sql

This creates a new database named "sa.sql". This what your database is assumed to be called. If you want to name it something different, you'll need to edit `sa/db.py:  default_db_path = os.getcwd() + "/../your_db.sql"`
