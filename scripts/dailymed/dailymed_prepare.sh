#!/usr/bin/env bash
alias python='python3'

# run this from the stitcher directory! (due to DailyMedParser dependency)
if [[ ! `pwd` == */stitcher ]]; then
	echo "Run this script from the main stitcher directory! Aborting."
	exit 1
fi

# check for key dependency
# get script dir
SCRIPT_DIR=$( dirname ${BASH_SOURCE[0]} )
if [ ! -e "$SCRIPT_DIR/dailymed_fix_otc.py" ]; then
	echo "dailymed_fix_otc.py is missing! Make sure it's in the same directory as this shell script. Aborting"
	exit 1
fi

# declare a directory where the original files are located
if [ $# -lt 1 ]; then
	echo "Please, supply paths to the directory with DailyMed zip files. Aborting."
    exit 1
else 
	save_to="$( cd $1 && pwd )/"
fi

######################################## declare/check ########################################
# declare all necessary files
files=(
		dm_spl_release_human_rx_part1.zip 
		dm_spl_release_human_rx_part2.zip 
		dm_spl_release_human_rx_part3.zip 
		dm_spl_release_human_rx_part4.zip
		dm_spl_release_human_rx_part5.zip
		dm_spl_release_human_otc_part1.zip
		dm_spl_release_human_otc_part2.zip
		dm_spl_release_human_otc_part3.zip
		dm_spl_release_human_otc_part4.zip
		dm_spl_release_human_otc_part5.zip
		dm_spl_release_human_otc_part6.zip
		dm_spl_release_human_otc_part7.zip
		dm_spl_release_human_otc_part8.zip
		dm_spl_release_human_otc_part9.zip
		dm_spl_release_human_otc_part10.zip
		dm_spl_release_remainder.zip
		dm_spl_release_animal.zip
		dm_spl_release_homeopathic.zip
		)
		
# append full paths to file names
files=( "${files[@]/#/$1}" ) # files=( "${files[@]/#/$save_to}" ) 

missing_files=()

# check if all files are there
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
		_rx
		_otc
		_rem
    _ani
		_homeo
		)

for type in ${types[@]}; do
	# select necessary files
	subset=`printf '%s\n' "${files[@]}" | grep "$type"`
	
	echo "Parsing $subset files..."
	
	# parse them
	sbt --error dailymed/"runMain ncats.stitcher.dailymed.DailyMedParser `echo $subset`" > stitcher-inputs/temp/spl$type.txt

	wait
	
	# leave only active compounds (otherwise stitching later will take too long)
	# by removing the ones with inactive (IACT) and NOT/MAY contain (CNTM) codes
	#cat spl$type.txt | sed '/\tIACT\t/d' | sed '/\tCNTM\t/d' | sed '/\tINGR\t/d' > spl_acti$type.txt
	
	# remove all lines starting with control elements (they are auxiliary)
	#sed -i '/^[[:cntrl:]]/ d' spl_acti$type.txt 
	#sed -i '/^java/ d' spl_acti$type.txt
	
	# gzip the original file [deprecated/unnecessary]
	#gzip spl$type.txt
	#tar -czf spl$type.tar.gz spl$type.txt 
done

echo "done parsing"
