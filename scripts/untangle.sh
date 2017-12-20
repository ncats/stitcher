#!/bin/sh
db="stitch.db"
version=1
component=
sbt stitcher/"runMain ncats.stitcher.UntangleCompoundComponent $db $version $component"