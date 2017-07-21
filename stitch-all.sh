#!/bin/sh

sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory stitch.db cache=data/hash.db jsonDump2016-08-05.gsrs"

sbt stitcher/"runMain ncats.stitcher.impl.RanchoJsonEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/rancho-export_2017-06-28_17-51.json.gz"

sbt stitcher/"runMain ncats.stitcher.impl.NPCEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"

sbt stitcher/"runMain ncats.stitcher.impl.PharmMenuEncyl3rdEntityFactory drugs.db ../inxight-planning/files/PharmManuEncycl3rdEd.json"

sbt stitcher/"runMain ncats.stitcher.impl.DrugBankEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/drugbank-full-annotated.sdf"

sbt stitcher/"runMain ncats.stitcher.impl.IntegrityMoleculeEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/integr.sdf.gz"

#sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory stitch.db data/tocris.conf"

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory stitch.db data/ruili.conf"
