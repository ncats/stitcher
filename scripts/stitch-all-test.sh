#!/bin/bash

db="stitchvBROAD.db"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > log.txt

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/broad.conf"
sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"

echo $(date) >> log.txt