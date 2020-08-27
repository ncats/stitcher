#!/bin/bash

# declare a directory where the original files will be saved 
if [ $# -lt 1 ]; then
	echo "Downloading DailyMed files into the current directory..."
	save_to=.
else 
	save_to=$( cd $1 && pwd )
fi

######################################## download ########################################
#change into that directory for now
pushd $save_to

#declare all files to be downloaded
files=(
		dm_spl_release_human_rx.zip 
		dm_spl_release_human_otc.zip
		dm_spl_release_remainder.zip
		dm_spl_release_animal.zip
		dm_spl_release_homeopathic.zip
		)

missing_files=()

#now get all those files
for f in ${files[@]}; do

	echo "Fetching $f..."
	curl -# -o $f "ftp://public.nlm.nih.gov/nlmdata/.dailymed/$f" &
	
	sleep 2 #sleep to allow the file to appear in the system
	
	if [ ! -f $f ]; then
		echo "Fetching $f didn't work!"
		missing_files+=($f)
	fi
	
done

wait

#check file sizes
for f in ${files[@]}; do
	#get file sizes
	fileSizeOrig=`curl -sI ftp://public.nlm.nih.gov/nlmdata/.dailymed/$f | grep Content-Length | cut -d' ' -f2`
	fileSizeCurrent=`stat --printf="%s" $f`

	if [ "$fileSizeOrig" = "$fileSizeCurrent" ]; then
		echo "WARNING: Download appears incomplete for $f"
	fi

done

echo "File integrity check complete."

#check if all files are there
if [ ${#missing_files[@]} -gt 0 ]; then
	echo "Some files could not be downloaded! Run the script again."
	exit 1
fi
