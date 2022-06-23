#!/usr/bin/env bash
set -ex
/usr/local/mysql/bin/mysql -u root --skip-password -S /etc/rondb/log/mysql.sock