
stitcherDataInxightRepo=${STITCHER_DATA_INXIGHT_DIRECTORY}
alias python='python3'
SCRIPT_DIR=$( dirname ${BASH_SOURCE[0]} )

if [[ ! $stitcherDataInxightRepo ]]; then
  echo "Please define the STITCHER_DATA_INXIGHT_DIRECTORY variable before running the script. Probably you should use the workflow in \"./workflows/Snakefile\""
  exit 1
fi

# process 'missing' labels
sbt --error dailymed/"runMain ncats.stitcher.dailymed.DailyMedParser $stitcherDataInxightRepo/files/spl-ndc/spl-missing-labels.zip" > stitcher-inputs/temp/spl_missing.txt 2> /dev/null

# create summary spl file
python3 $SCRIPT_DIR/dailymed_merge_ndc.py # produces data/spl_summary.txt

# process inactivated labels
sbt --error dailymed/"runMain ncats.stitcher.dailymed.DailyMedParser stitcher-inputs/temp/fda_initiated_inactive_ndcs_indexing_spl_files.zip" > stitcher-inputs/temp/spl_inactivated.txt 2> /dev/null

# compare with otc_monograph_final, and remove UNIIs that don't belong
#echo "Fixing OTC file..."
#python $SCRIPT_DIR/dailymed_fix_otc.py spl_acti_otc.txt data/otc_monograph_final_all.xls
#wait

echo "All done!"

