#!/usr/bin/env bash
timestamp="$(date +'%Y%m%d-%H%M%S')"
db="stitchv${timestamp}.db"
dbzip="stitchv${timestamp}db.zip"
log="log${timestamp}.txt"

#keep track of current time
curr_time=$(date +%s)

echo $(date) > $log

#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/russian_medicines.conf"
#echo 'Russian medicines:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/withdrawn.conf"
echo 'Withdrawn:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/broad.conf"
echo 'Broad:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/ruili.conf"
#echo 'Ruili:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db \"name=G-SRS, Apr 2020\" cache=data/hash.db ../stitcher-rawinputs/files/dump-public-2020-04-28.gsrs"
echo 'gsrs:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db \"name=Rancho BioSciences, Sept 2019\" cache=data/hash.db ../stitcher-rawinputs/files/rancho-export_2019-09-27_13-00.json"
echo 'rancho:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.NPCEntityFactory $db \"name=NCATS Pharmaceutical Collection, April 2012\" cache=data/hash.db ../stitcher-rawinputs/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"
echo 'NPC:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory $db \"name=Pharmaceutical Manufacturing Encyclopedia (Third Edition)\" ../stitcher-rawinputs/files/PharmManuEncycl3rdEd.json"
echo 'PharmManuEncycl:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.DrugBankXmlEntityFactory $db \"name=DrugBank, December 2018\" cache=data/hash.db ../stitcher-rawinputs/files/drugbank_all_full_database.xml.zip"
echo 'DrugBank:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log


# these add additional data for event calculator
sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrx.conf"
echo 'DailyMedRx:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrem.conf"
echo 'DailyMedRem:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedotc.conf"
echo 'DailyMedOTC:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ob.conf"
echo 'OB:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ct.conf"
echo 'CT:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/otc.conf"
echo 'OTC:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/FDAanimalDrugs.conf"
echo 'NADA:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/FDAexcipients.conf"
echo 'IIG:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

# make db copy before stitching...
#cp -r $db NOSTITCH$db

# now the stitching...
sbt -mem 16000 stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
echo 'Stitching:' $(( ($(date +%s) - $curr_time )/60 )) 'min' >> $log

echo $(date) >> $log


# zip up the directory and copy over to centos
# zip -r $dbzip $db
# scp $dbzip centos@dev.ncats.io:/tmp

