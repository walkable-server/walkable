#!/bin/bash
set -ev

case "$1" in
    unit)
        lein install
        ;;
    sqlite)
        lein with-profile dev install
        npm install
        ;;
    postgres)
        psql -c 'create database walkable_dev;' -U postgres
        lein with-profile dev install
        ;;
    mysql)
        mysql -e 'CREATE DATABASE IF NOT EXISTS walkable_dev;'
        lein with-profile dev install
        ;;
    *)
        echo $"Usage: $0 {unit|sqlite|mysql|postgres}"
        exit 1
esac
