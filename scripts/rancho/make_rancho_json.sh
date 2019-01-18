#!/bin/bash

#check that an argument has been supplied
if [ $# -eq 0 ]; then
    echo "Please, supply path to the .tsv file with Rancho export. Aborting."
    exit 1
fi


 
#run the script 
python `dirname ${BASH_SOURCE[0]}`/rancho_tsv2json.py $1 > "rancho-`basename ${1/%tsv/json}`"