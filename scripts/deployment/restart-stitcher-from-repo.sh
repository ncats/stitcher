#! /bin/bash

if [ $# -lt 1 ]; then
    echo "Please, supply the path to the database."
    echo "Aborting."
    exit 1
fi

db=$( cd $1 && pwd )
pidfile=RUNNING_PID

if [ -e $pidfile ]; then
    #kill the running process
    kill -TERM $( cat "$pidfile" )
fi

sleep 3

#remove symlink to the old database and dist
rm -rf ./stitcher.ix/data.db

if [ ! -e stitcher.ix ]; then
	if [ ! -e files-for-stitcher.ix ]; then
		echo "Could not find neither 'stitcher.ix' nor 'files-for-stitcher.ix'."
		echo "Aborting."
		exit 1	
	fi
	
    mkdir stitcher.ix
    cp files-for-stitcher.ix/* stitcher.ix
fi

#create symlink to a new database (and other symlinks if missing)
ln -s $db ./stitcher.ix/data.db

#restart stitcher app/api
./activator run \
 -Dix.version.latest=1 \
 -Dhttp.port=9003 \
 -Dplay.http.secret.key=`head -c2096 /dev/urandom | sha256sum |cut -d' ' -f1`