#!/bin/bash

scp $1 stitcher@stitcher-dev.ncats.io:/work
wait
scp $1 centos@stitcher.ncats.io:/tmp
wait

