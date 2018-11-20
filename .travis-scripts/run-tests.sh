#!/bin/bash
set -ev

case "$1" in
    unit)
        lein test
        ;;
    sqlite)
        lein with-profile sqlite test :integration
        ./node_modules/.bin/shadow-cljs compile test
        node test.js
        ;;
    postgres)
        lein with-profile postgres test :integration
        ;;
    mysql)
        lein with-profile mysql test :integration
        ;;
    *)
        echo $"Usage: $0 {unit|sqlite|mysql|postgres}"
        exit 1
esac
