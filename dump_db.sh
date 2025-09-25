#!/usr/bin/bash
DB=changes.sqlite
for t in branches tags files commits backports changes
do
        sqlite3 $DB ".schema $t"
        sqlite3 $DB "select * from $t"
        echo
done
