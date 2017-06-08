#!/bin/sh

sbt stitcher/"runMain ncats.stitcher.impl.SRSJsonEntityFactory stitch.db cache=data/hash.db jsonDump2016-08-05.gsrs"

sbt stitcher/"runMain ncats.stitcher.impl.DrugBankEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/drugbank-full-annotated.sdf"

sbt stitcher/"runMain ncats.stitcher.impl.IntegrityMoleculeEntityFactory stitch.db cache=data/hash.db ../inxight-planning/files/integr.sdf.gz"

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory stitch.db data/tocris.conf"

sbt stitcher/"runMain ncats.stitcher.impl.LineMoleculeEntityFactory stitch.db data/ruili.conf"
