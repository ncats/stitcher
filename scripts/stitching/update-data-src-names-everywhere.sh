#!/usr/bin/env bash

# run this from the stitcher directory! (due to DailyMedParser dependency)
if [[ ! `pwd` == */stitcher ]]; then
	echo "Run this script from the main stitcher directory! Aborting."
	exit 1
fi

# check args
if [ ! $# -eq 2 ]; then
    echo "Please, supply (1) a datasource nickname and (2) replacement string."
    echo "Aborting."
    exit 1
fi

ds=$1
repl=$2

files=`grep -irwnl -e $ds \
--exclude-dir={*git,target,*.db} \
--exclude={.gitignore,*.jar}`

#ignore case, get only matching pattern, use Perl, omit file names
str2repl=`grep -ioPh '(?<=name=).*'$ds'.*(?=\\\" )' $files | head -n1` 

if [[ $str2repl == '' ]] 
then 
	echo "Couldn't find the string to replace! Aborting."
	exit 1
fi

echo "Going to replace string '$str2repl' with '$repl'."
read -p "OK to proceed? (y/n)" -n 1 -r
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1 
fi
echo

for f in $files
do
	sed -i "s/$str2repl/$repl/g" "$f"
	echo "Processed file: $f"
done

echo "All done!"