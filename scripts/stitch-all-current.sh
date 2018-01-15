#!/bin/bash

db="stitchv20180115.db"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > log.txt
sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/broad.conf"
echo 'Broad:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/ruili.conf"
echo 'Ruili:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db cache=data/hash.db data/dump-public-2017-10-14.gsrs"
echo 'gsrs:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db cache=data/hash.db data/rancho-export_2017-10-26_20-39.json"
echo 'rancho:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.NPCEntityFactory $db cache=data/hash.db ../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"
echo 'NPC:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory $db ../inxight-planning/files/PharmManuEncycl3rdEd.json"
echo 'PharmManuEncycl:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.DrugBankXmlEntityFactory $db cache=data/hash.db ../inxight-planning/files/drugbank_all_full_database.xml.zip"
echo 'DrugBank:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt

#sbt stitcher/"runMain ncats.stitcher.impl.IntegrityMoleculeEntityFactory $db cache=data/hash.db ../inxight-planning/files/integr.sdf.gz"
#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/tocris.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/ruili.conf"
# these add additional data for event calculator
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrx.conf"
echo 'DailyMedRx:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrem.conf"
echo 'DailyMedRem:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedotc.conf"
echo 'DailyMedOTC:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ob.conf"
echo 'OB:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ct.conf"
echo 'CT:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
# now the stitching..
sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
echo 'Stitching:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
# calculate events
sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"
echo 'EventCalculator:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> log.txt
echo $(date) >> log.txt
