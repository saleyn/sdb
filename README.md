# SecDB - database for storing streaming market data #

Data storage format and supporting Linux tools for financial market data.
It's designed to represent snapshots of the multi-level order book and trades.
The format is documented here: https://github.com/saleyn/secdb/wiki/Data-Format

The SecDB tools include:

* `secdb-krx` - convert KRX market data from text to `sdb` data format
* `secdb` - print the content of `sdb` database file

## Author ##

    Serge Aleynikov <saleyn@gmail.com>

## Installation ##

Dependencies:
* `boost` version 1.56 or above
* `utxx` found here: https://github.com/saleyn/utxx.git

```
git clone https://github.com/saleyn/secdb.git
cd secdb
make bootstrap
make
make install
```

## LICENSE ##

The project is dual-licensed under AGPL for open-source non-commercial use, as well
as commercially licensed for commercial use (contact the author for details).
