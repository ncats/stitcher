#!/bin/bash

#run this from stitcher directory!
if [[ ! `pwd` == */stitcher ]]; then
	echo "Run this script from the main stitcher directory! Aborting."
	exit 1
fi

######################################## declare/check ########################################

#declare a directory where the original files are located
#ATTN: need '/' at the end!
save_to=../

#declare all necessary files
files=(
		dm_spl_release_remainder.zip
		dm_spl_release_human_rx_part1.zip 
		dm_spl_release_human_rx_part2.zip 
		dm_spl_release_human_rx_part3.zip
		dm_spl_release_human_otc_part1.zip 
		dm_spl_release_human_otc_part2.zip 
		dm_spl_release_human_otc_part3.zip 
		dm_spl_release_human_otc_part4.zip 
		dm_spl_release_human_otc_part5.zip
		)
		
#append full paths to file names
files=( "${files[@]/#/$save_to}" ) 

missing_files=()

#check if all files are there
for f in ${files[@]}; do
	if [ ! -f $f ]; then
		echo "File $f is missing!"
		missing_files+=($f)
	fi
done

if [ ${#missing_files[@]} -gt 0 ]; then
	echo "Some files are missing! Aborting."
	exit 1
fi

######################################## parse/prepare ########################################
types=(
		_rem
		_rx
		_otc)

for type in ${types[@]}; do
	#select necessary files
	subset=`printf '%s\n' "${files[@]}" | grep "$type"`
	
	echo "Parsing $subset files..."
	
	#parse them
	sbt dailymed/"runMain ncats.stitcher.dailymed.DailyMedParser `echo $subset`" > spl$type.txt 
	
	wait
	
	#leave only active compounds (otherwise stitching later will take too long)
	cat spl$type.txt | sed '/\tIACT\t/d' > spl_acti$type.txt
	
	#remove all lines starting with control elements (they are auxiliary)
	sed -ie '/^[[:cntrl:]]/ d' spl_acti$type.txt 
	sed -ie '/^java/ d' spl_acti$type.txt
	
	#gzip the original file
	gzip spl$type.txt 
done
