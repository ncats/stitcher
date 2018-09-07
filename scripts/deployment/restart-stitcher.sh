#! /bin/bash

if [ $# -lt 2 ]; then
    echo "Please, supply paths to the following:"
    echo "a) current dist;"
    echo "b) database;"
    echo "c) [optional] new dist."
    echo "Aborting."
    exit 1
elif [ $# -gt 2 ]; then
    dist=$( cd $3 && pwd )
    curr_dist=$( cd $1 && pwd )
elif [ $# -eq 2 ]; then
    dist=$( cd $1 && pwd )
    curr_dist=$dist
fi

db=$( cd $2 && pwd )
pidfile=${curr_dist}/RUNNING_PID

if [ -e $pidfile ]; then
    #kill the running process
    kill -TERM $( cat "$pidfile" )
fi

sleep 3

#remove symlink to the old database and dist
rm -rf ./stitcher.ix/data.db
rm -rf latest

if [ ! -e stitcher.ix ]; then
    mkdir stitcher.ix
    cp files-for-stitcher.ix/* stitcher.ix
fi

#create symlink to a new database (and other symlinks if missing)
ln -s $db ./stitcher.ix/data.db
ln -s $dist latest

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