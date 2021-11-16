#!/usr/bin/env bash

# check arguments
if [ $# -lt 1 ]; then
	echo "Please, supply (1) a UNII, (2) path to the GSRS dump [optional]. Aborting."
    exit 1
fi

# specify vars
unii=$1
gsrs_gz=$2

# if the script was previously run 
gsrs_json="/tmp/gsrs_json"
gsrs_unii="/tmp/gsrs_unii"
gsrs_extract="/tmp/gsrs_extract"
# default 
gsrs_default_dir="../stitcher-rawinputs/files"
outdir="."

# unzip the dump
if [ ! -e "$gsrs_json" ]; then
	if [ -e "$gsrs_gz" ]; then
		echo "Unzipping the gsrs file..."
		gunzip -c "$gsrs_gz" > "$gsrs_json"
	else
		echo "Could not find a GSRS json or the original dump. Aborting."
		echo "You can supply path to a GSRS dump as a third argument."
		exit 1
	fi
else
	echo "Using exisiting GSRS json located at $gsrs_json"
	outdir="/tmp"
fi

if [ ! -e "$gsrs_unii" ]; then
	cat "$gsrs_json" | jq '.approvalID' > "$gsrs_unii"
else
	echo "Using exisiting UNII list in $gsrs_unii"
fi

echo "Looking for a requested UNII in the UNII list..."
unii_line=`cat "$gsrs_unii" | grep -n $unii | cut -d : -f 1`

if [[ -z "${unii_line}" &&  $unii_line -ne 1 ]]; then
	echo "Could not find just one substance entry in GSRS. Aborting."
	exit 1
else 
	echo "UNII found on line: $unii_line"
fi

gsrs_record=`sed $unii_line'q;d' "$gsrs_json"`

# make output filename as "dump-public-[UNII].gsrs"
out_file="$outdir/dump-public-$unii.gsrs"
echo "Writing to file $out_file ..."

echo "$gsrs_record" > "$gsrs_extract"


gzip -c "$gsrs_extract" > "$out_file"
echo "Done! The GSRS extract is located here: $out_file"