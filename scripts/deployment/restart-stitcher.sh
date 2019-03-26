#! /bin/bash

if [ $# -lt 1 ]; then
    echo "Please, supply paths to the database and [optional] new dist."
    echo "Aborting."
    exit 1
elif [ $# -gt 1 ]; then
    new=$( cd $2 && pwd )
elif [ $# -eq 1 ]; then
    if [ -e latest ]; then
        new=$( cd latest && pwd -P )
    else
        echo "Could not find a current distribution."
        echo "Please, restart and supply path to the database and the dist."
        exit 1
    fi
fi

db=$( cd $1 && pwd )
pidfile=latest/RUNNING_PID

if [ -e $pidfile ]; then
    #kill the running process
    kill -TERM $( cat "$pidfile" )
fi

sleep 3

#remove symlink to the old database and dist
rm -rf ./stitcher.ix/data.db
rm -rf latest

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
ln -s $new latest

if [ ! -e ${new}/data.db ]; then
    ln -s ../stitcher.ix/data.db ${new}/data.db
fi

if [ ! -e ${new}/stitcher.ix ]; then
    ln -s ../stitcher.ix ${new}/stitcher.ix
fi

#restart stitcher app/api
cd latest
nohup ./bin/ncats-stitcher \
 -Dix.version.latest=1 \
 -Dhttp.port=9003 \
 -Dplay.http.secret.key=`head -c2096 /dev/urandom | sha256sum |cut -d' ' -f1` &
