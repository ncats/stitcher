#!/bin/sh

version="v2"
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
   ogg.owl.gz \
   ogms.owl \
   pato.owl.gz \
   pr.owl.gz"
owl_path="owl"
owl_files=`echo $owl | xargs printf " ${owl_path}/%s"`
#echo $owl_files

out="ncatskg-$version.db"
cache="cache=hash.db"

#load GARD
gard_credentials=
if test -f "gard-credentials.txt"; then
    gard_credentials=`cat gard-credentials.txt`
fi
sbt stitcher/"runMain ncats.stitcher.impl.GARDEntityFactory\$Register $out $gard_credentials"

#load GHR
sbt stitcher/"runMain ncats.stitcher.impl.GHREntityFactory $out"

# load ontologies
#sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $owl_files"
for f in $owl_files; do
    sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $f"
done

#load ChEBI
sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $cache $owl_path/chebi.xrdf.gz"

#load rancho
sbt stitcher/"runMain ncats.stitcher.impl.InxightEntityFactory $out $cache data/rancho-disease-drug_2018-12-18_13-30.txt"

# load orphan designation
sbt stitcher/"runMain ncats.stitcher.impl.FDAOrphanDesignationEntityFactory $out data/FDAOrphanGARD_20190216.txt"

#load hpo annotations
sbt stitcher/"runMain ncats.stitcher.impl.HPOEntityFactory $out data/HPO_annotation_100918.txt"
