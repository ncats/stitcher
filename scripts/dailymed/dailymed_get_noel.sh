#!/bin/bash

curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_animal.zip' -o ../../temp/dm_spl_release_animal.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_homeopathic.zip' -o ../../temp/dm_spl_release_homeopathic.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_otc.zip' -o ../../temp/dm_spl_release_human_otc.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_human_rx.zip' -o ../../temp/dm_spl_release_human_rx.zip
curl 'ftp://public.nlm.nih.gov/nlmdata/.dailymed/dm_spl_release_remainder.zip' -o ../../temp/dm_spl_release_remainder.zip