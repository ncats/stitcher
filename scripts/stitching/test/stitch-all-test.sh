#!/bin/bash
timestamp="$(date +'%Y%m%d-%H%M%S')"
db="stitchv-TEST-${timestamp}.db"
dbzip="${db}.zip"
log="log-TEST-${timestamp}.txt"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > $log

sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db \"name=FRDB, October 2021\" cache=data/hash.db data/test/frdb_test_UMBRALISIB.json"
echo 'rancho:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log
sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db \"name=G-SRS, May 2021\" cache=data/hash.db data/test/dump-public-38073MQB2A.gsrs"
echo 'gsrs:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory stitchvTEST.db data/ob.conf"
#sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
#sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"

# now the stitching...
sbt -mem 32000 stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
echo 'Stitching:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

#zip -r $dbzip $db

echo $(date) >> $log