#!/bin/bash

if [[ ! -d ../stitcher-inputs ]]; then
  mkdir ../stitcher-inputs
fi

if [[ ! -d ../stitcher-inputs/temp ]]; then
  mkdir ../stitcher-inputs/temp
fi

# https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_animal.zip' -o ../stitcher-inputs/temp/dm_spl_release_animal.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_homeopathic.zip' -o ../stitcher-inputs/temp/dm_spl_release_homeopathic.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part1.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part1.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part2.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part2.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part3.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part3.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part4.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part4.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part5.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part5.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part6.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part6.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part7.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part7.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part8.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part8.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part9.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part9.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part10.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_otc_part10.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part1.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_rx_part1.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part2.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_rx_part2.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part3.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_rx_part3.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part4.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_rx_part4.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part5.zip' -o ../stitcher-inputs/temp/dm_spl_release_human_rx_part5.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_remainder.zip' -o ../stitcher-inputs/temp/dm_spl_release_remainder.zip


# https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-indexing-files.cfm
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/fda_initiated_inactive_ndcs_indexing_spl_files.zip' -o ../stitcher-inputs/temp/fda_initiated_inactive_ndcs_indexing_spl_files.zip
