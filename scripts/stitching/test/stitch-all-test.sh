#!/bin/bash

db="stitchvTEST.db"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > log.txt

sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db \"name=G-SRS, February 2018\" cache=data/hash.db data/dump-public-2018-02-06-TEST.gsrs"
sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"

echo $(date) >> log.txt