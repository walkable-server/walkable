#!/bin/bash
set -ev

case "$1" in
    unit)
        lein install
        ;;
    sqlite)
        lein with-profile sqlite install
        npm install
        ;;
    postgres)
        psql -c 'create database walkable_dev;' -U postgres
        lein with-profile postgres install
        ;;
    mysql)
        mysql -e 'CREATE DATABASE IF NOT EXISTS walkable_dev;'
        lein with-profile mysql install
        ;;
    *)
        echo $"Usage: $0 {unit|sqlite|mysql|postgres}"
        exit 1
esac
