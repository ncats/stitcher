#!/bin/bash

#run this from stitcher directory!
if [[ ! `pwd` == */stitcher ]]; then
	echo "Run this script from the main stitcher directory! Aborting."
	exit 1
fi

######################################## download ########################################

#declare a directory where the original files will be saved 
#ATTN: need '/' at the end!
save_to=./scripts/

#change into that directory for now
pushd $save_to

#declare all files to be downloaded
files=(dm_spl_release_human_rx_part1.zip 
		dm_spl_release_human_rx_part2.zip 
		dm_spl_release_human_rx_part3.zip
		dm_spl_release_human_otc_part1.zip 
		dm_spl_release_human_otc_part2.zip 
		dm_spl_release_human_otc_part3.zip 
		dm_spl_release_human_otc_part4.zip 
		dm_spl_release_human_otc_part5.zip
		dm_spl_release_remainder.zip)

missing_files=()

#now get all those files
for f in ${files[@]}; do
	i=0
	
	#sometimes download on first try fails, so multiple tries are required
	while [ ! -f $f ] && [ $i -lt 10 ]; do
		echo "Fetching $f (try #$i)..."
		wget -o /dev/null ftp://public.nlm.nih.gov/nlmdata/.dailymed/$f &
		sleep 3
		i=$(( $i+1 ))
	done
	
	if [ $i -gt 9 ]; then
		echo "We tried fetching $f $i times, and it didn't work!"
		missing_files+=($f)
	fi
	
done

wait

#check if all files are there
if [ ${#missing_files[@]} -gt 0 ]; then
	echo "Some files could not be downloaded! Run the script again."
	exit 1
fi

#pop back into stitcher directory
popd

#append full paths to file names
files=( "${files[@]/#/$save_to}" ) 

######################################## parse/prepare ########################################
echo "Now to parsing."
types=(_rx
		_otc
		_rem)

for type in ${types[@]}; do
	#select necessary files
	subset=`printf '%s\n' "${files[@]}" | grep "$type"`
	
	echo "Parsing $subset files..."
	
	#parse them
	sbt dailymed/"runMain ncats.stitcher.dailymed.DailyMedParser `echo $subset`" > spl$type.txt 
	
	wait
		
	#remove first 3 lines (they are auxiliary)
	sed -i 1,3d spl$type.txt 
	
	#leave only active compounds (otherwise stitching later will take too long)
	cat spl$type.txt | sed '/\tIACT\t/d' > spl_acti$type.txt
	
	#gzip the original file
	gzip spl$type.txt 
done
