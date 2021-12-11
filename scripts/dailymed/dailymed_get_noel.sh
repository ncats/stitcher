#!/bin/bash

# https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm
# https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-indexing-files.cfm

curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_animal.zip' -o ../../temp/dm_spl_release_animal.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_homeopathic.zip' -o ../../temp/dm_spl_release_homeopathic.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part1.zip' -o ../../temp/dm_spl_release_human_otc_part1.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part2.zip' -o ../../temp/dm_spl_release_human_otc_part2.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part3.zip' -o ../../temp/dm_spl_release_human_otc_part3.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part4.zip' -o ../../temp/dm_spl_release_human_otc_part4.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part5.zip' -o ../../temp/dm_spl_release_human_otc_part5.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part6.zip' -o ../../temp/dm_spl_release_human_otc_part6.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part7.zip' -o ../../temp/dm_spl_release_human_otc_part7.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part8.zip' -o ../../temp/dm_spl_release_human_otc_part8.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part9.zip' -o ../../temp/dm_spl_release_human_otc_part9.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc_part10.zip' -o ../../temp/dm_spl_release_human_otc_part10.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part1.zip' -o ../../temp/dm_spl_release_human_rx_part1.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part2.zip' -o ../../temp/dm_spl_release_human_rx_part2.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part3.zip' -o ../../temp/dm_spl_release_human_rx_part3.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx_part4.zip' -o ../../temp/dm_spl_release_human_rx_part4.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_remainder.zip' -o ../../temp/dm_spl_release_remainder.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/fda_initiated_inactive_ndcs_indexing_spl_files.zip' -o ../../temp/fda_initiated_inactive_ndcs_indexing_spl_files.zip
