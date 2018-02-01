#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Please, supply path to the stitcher database as an argument. Aborting."
    exit 1
fi

newdb=$1

#kill the running process
kill -TERM `cat RUNNING_PID`

#remove old database (can be either symlink or directory
rm -r ../stitcher.ix/data.db
rm ../stitcher.ix/data.db

#create symlink to a new database
ln -s $newdb ../stitcher.ix/data.db

#restart stitcher app/api
sh ../stitcher.sh
