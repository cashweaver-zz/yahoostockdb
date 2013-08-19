stock_analyzer
==============

Python based stock analysis. Abandoned in favor of a C++ implementation, because I like the pain.

Installation
============

This guide was written for Ubuntu 12.04. Your mileage may vary.

Dependencies
------------

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
    sudo pip install configparser
```

All the commands as one line:
```bash
    sudo apt-get -y install python-dev python-pip; sudo pip install cython; sudo pip install numpy; wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz; tar -xzf ta-lib-0.4.0-src.tar.gz; cd ta-lib; ./configure --prefix=/usr; make; sudo make install; sudo pip install TA-lib; sudo pip install ystockquote; sudo pip install configparser;
```
