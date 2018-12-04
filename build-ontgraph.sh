#!/bin/sh

owl="BrendaTissue.owl.gz \
   DOID.owl.gz \
   HPO.owl.gz \
   MEDLINEPLUS.ttl.gz \
   MESH.ttl.gz \
   MONDO.owl.gz \
   OMIM.ttl.gz \
   UBERON.owl.gz \
   ordo.owl.gz \
   GO.owl.gz"
owl_path="owl"
owl_files=`echo $owl | xargs printf " ${owl_path}/%s"`
#echo $owl_files

out="ncatsowl.db"
sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $owl_files"
