#!/bin/bash
set -ev

case "$1" in
    unit)
        lein test
        ;;
    sqlite)
        lein with-profile dev test :sqlite
        ./node_modules/.bin/shadow-cljs compile test
        node test.js
        ;;
    postgres)
        lein with-profile dev test :postgres
        ;;
    mysql)
        lein with-profile dev test :mysql
        ;;
    *)
        echo $"Usage: $0 {unit|sqlite|mysql|postgres}"
        exit 1
esac
