#!/bin/bash
timestamp="$(date +'%Y%m%d-%H%M%S')"
db="stitchv-TEST-${timestamp}.db"
dbzip="${db}.zip"
log="log-TEST-${timestamp}.txt"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > $log

sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db \"name=FRDB, March 2021\" cache=data/hash.db ../stitcher-rawinputs/files/frdb_2021-03-16.json"
echo 'rancho:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log
sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db \"name=G-SRS, April 2020\" cache=data/hash.db ../stitcher-rawinputs/files/dump-public-2020-04-28.gsrs"
echo 'gsrs:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory stitchvTEST.db data/ob.conf"
#sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
#sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"

# now the stitching...
sbt -mem 32000 stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
echo 'Stitching:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

zip -r $dbzip $db

echo $(date) >> $log