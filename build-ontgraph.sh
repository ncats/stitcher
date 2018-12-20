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
   GO.owl.gz \
   umls_semantictypes.ttl"
owl_path="owl"
owl_files=`echo $owl | xargs printf " ${owl_path}/%s"`
#echo $owl_files

out="ncatskg.db"
# load ontologies
sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $owl_files"

#load rancho
sbt stitcher/"runMain ncats.stitcher.impl.InxightEntityFactory $out data/rancho-disease-drug_2018-12-18_13-30.txt"

#load hpo annotations
sbt stitcher/"runMain ncats.stitcher.impl.HPOEntityFactory $out data/HPO_annotation_100918.txt
