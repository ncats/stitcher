#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Please, supply paths to the stitcher dist and the database as arguments. Aborting."
    exit 1
fi

dist=$( cd $1 && pwd )
db=$( cd $2 && pwd )
pidfile=${dist}/RUNNING_PID

#kill the running process (if running)
if [ -e $pidfile ]; then
    kill -TERM $( cat "$pidfile" )
fi

sleep 3 #it might take a moment

#remove symlinks to the old database and dist
rm -rf ./stitcher.ix/data.db
rm -rf latest

if [ ! -e stitcher.ix ]; then
    mkdir stitcher.ix
    cp files-for-stitcher.ix/* stitcher.ix
fi

#create symlinks to the dist and the new database
ln -s $db ./stitcher.ix/data.db
ln -s $dist latest

#create other symlinks (if missing)
if [ ! -e ${dist}/data.db ]; then
    ln -s ../stitcher.ix/data.db ${dist}/data.db
fi

if [ ! -e ${dist}/stitcher.ix ]; then
    ln -s ../stitcher.ix ${dist}/stitcher.ix
fi

#restart stitcher app/api
nohup ${dist}/bin/ncats-stitcher \
 -Dix.version.latest=1 \
 -Dhttp.port=9003 \
 -Dplay.http.secret.key=`head -c2096 /dev/urandom | sha256sum |cut -d' ' -f1` &