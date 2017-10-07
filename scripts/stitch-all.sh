#!/bin/sh

db="stitchv1.db"

#sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db cache=data/hash.db jsonDump2016-08-05.gsrs"
sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory $db cache=data/hash.db dump-public-2017-10-04.gsrs"

#sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db cache=data/hash.db ../inxight-planning/files/rancho-export_2017-06-28_17-51.json.gz"
sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory $db cache=data/hash.db rancho_2017-09-07_20-32.json"

sbt stitcher/"runMain ncats.stitcher.impl.NPCEntityFactory $db cache=data/hash.db ../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"

sbt stitcher/"runMain ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory $db ../inxight-planning/files/PharmManuEncycl3rdEd.json"

sbt stitcher/"runMain ncats.stitcher.impl.DrugBankEntityFactory $db cache=data/hash.db ../inxight-planning/files/drugbank-full-annotated.sdf"

#sbt stitcher/"runMain ncats.stitcher.impl.IntegrityMoleculeEntityFactory $db cache=data/hash.db ../inxight-planning/files/integr.sdf.gz"

#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/tocris.conf"

#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory $db data/ruili.conf"

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/dailymedrx.conf"

sbt stitcher/"runMain ncats.stitcher.impl.MapEntityFactory $db data/ct.conf"
