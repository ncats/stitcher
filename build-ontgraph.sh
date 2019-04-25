#!/bin/sh

version="v7"
out="ncatskg-$version.db"
cache="cache=hash.db"
orphclass="orphanet_classifications"
medgen="medgen"
ppi="ppi"

###########################
##### DON'T MESS BELOW
###########################
# make sure MONDO is last in disease ontologies
owl="DOID.owl.gz \
   HPO.owl.gz \
   MEDLINEPLUS.ttl.gz \
   MESH.ttl.gz \
   OMIM.ttl.gz \
   ordo.owl.gz \
   Thesaurus.owl.gz \
   MONDO.owl.gz \
   BrendaTissue.owl.gz \
   UBERON.owl.gz \
   GO.owl.gz \
   ogg.owl.gz \
   ogms.owl \
   pato.owl.gz \
   pr.owl.gz"
owl_path="owl"
owl_files=`echo $owl | xargs printf " ${owl_path}/%s"`
#echo $owl_files

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

#load additional orphanet relationships if available
if test -d $orphclass; then
    sbt stitcher/"runMain ncats.stitcher.impl.OrphanetClassificationEntityFactory $out $orphclass"
fi

#load MedGen if available
if test -d $medgen; then
    sbt stitcher/"runMain ncats.stitcher.impl.MedGenEntityFactory $out $medgen"
fi

#load PPI if available
if test -d $ppi; then
    for f in $ppi/*.txt.gz; do
        sbt stitcher/"runMain ncats.stitcher.impl.PPIEntityFactory $out $f"
    done
fi
