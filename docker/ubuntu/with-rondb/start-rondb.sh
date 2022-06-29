#!/usr/bin/env bash

set -ex

/usr/local/mysql/bin/ndb_mgmd --ndb-nodeid=49 -f /etc/rondb/rondb.cnf --configdir=/etc/rondb/mgmd --reload --initial

sleep 10

/usr/local/mysql/bin/ndbmtd -c 0.0.0.0:1186 --ndb-nodeid=1 --connect-retries=-1 --connect-delay=10

/usr/local/mysql/bin/mysqld --defaults-file=/etc/rondb/my.cnf --initialize-insecure --explicit_defaults_for_timestamp

/usr/local/mysql/bin/mysqld --defaults-file=/etc/rondb/my.cnf > /etc/rondb/log/mysql_52_out.log  2>&1 < /dev/null &

sleep 10

ps -eaf 