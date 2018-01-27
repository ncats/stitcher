#!/bin/bash

db="stitchv2.db"

sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db cache=data/hash.db dump-public-2017-10-14.gsrs"
sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db cache=data/hash.db rancho-export_2017-10-26_20-39.json"
sbt stitcher/"runMain ncats.stitcher.impl.NPCEntityFactory $db cache=data/hash.db ../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"
sbt stitcher/"runMain ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory $db ../inxight-planning/files/PharmManuEncycl3rdEd.json"
sbt stitcher/"runMain ncats.stitcher.impl.DrugBankXmlEntityFactory $db cache=data/hash.db ../inxight-planning/files/drugbank_all_full_database.xml.zip"
#sbt stitcher/"runMain ncats.stitcher.impl.IntegrityMoleculeEntityFactory $db cache=data/hash.db ../inxight-planning/files/integr.sdf.gz"
#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/tocris.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/ruili.conf"
# these add additional data for event calculator
#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrx.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrem.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedotc.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ob.conf"
#sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ct.conf"
# now the stitching..
#sbt stitcher/"runMain ncats.stitcher.tools.CompoundStitcher $db 1"
# calculate events
#sbt stitcher/"runMain ncats.stitcher.calculators.EventCalculator $db 1"

