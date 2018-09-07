#! /bin/bash

for INDEX in {1..5}
do
	scripts/special-purpose/stitch-all-set${INDEX}.sh > stitchv$(date +'%Y%m%d-%H%M')SET${INDEX}.out 2>&1
done
