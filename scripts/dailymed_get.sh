#!/bin/bash
######################################## download ########################################

#declare a directory where the original files will be saved 
save_to=../

#change into that directory for now
pushd $save_to

#declare all files to be downloaded
files=(
		dm_spl_release_human_rx_part1.zip 
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
	
	
	echo "Fetching $f..."
	wget -nc -o /dev/null ftp://public.nlm.nih.gov/nlmdata/.dailymed/$f &
	
	sleep 2 #sleep to allow the file to appear in the system
	
	if [ ! -f $f ]; then
		echo "Fetching $f didn't work!"
		missing_files+=($f)
	fi
	
done

wait

#check if all files are there
if [ ${#missing_files[@]} -gt 0 ]; then
	echo "Some files could not be downloaded! Run the script again."
	exit 1
fi
