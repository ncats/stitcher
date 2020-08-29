#!/bin/sh

opts='-mem 16384'
version="v`date +%Y%m%d`"
out="ncatskg-$version.db"
cache="cache=hash.db"
orphclass="orphanet_classifications"
orpha="orpha"
hpo="hpo"
# medgen if available
medgen="medgen"
#clinvar if available
clinvar="clinvar/ClinVarVariationRelease_00-latest.xml.gz"
#this might be too much right now
ppi="ppi/BIOGRID-MV-Physical-3.5.172.mitab.txt.gz"

###########################
##### DON'T MESS BELOW
###########################

# make sure MONDO is last in disease ontologies
owl="doid.owl.gz \
   hp.owl.gz \
   MEDLINEPLUS.ttl.gz \
   MESH.ttl.gz \
   OMIM.ttl.gz \
   ICD10CM.ttl.gz \
   ordo_orphanet.owl.gz \
   ORDO_es_2.9.owl.gz \
   Thesaurus.owl.gz \
   VANDF.ttl.gz \
   bto.owl.gz \
   clo.owl.gz \
   cl.owl.gz \
   ddiem.owl.gz \
   uberon.owl.gz \
   go.owl.gz \
   geno.owl.gz \
   ogg.owl.gz \
   pw.owl.gz \
   mp.owl.gz \
   oae.owl.gz \
   rxno.owl.gz \
   ogms.owl \
   pato.owl.gz \
   fma.owl.gz"
owl_path="owl-202002"
owl_files=`echo $owl | xargs printf " ${owl_path}/%s"`
#echo $owl_files

#load GARD
gard_credentials=
if test -f "gard-credentials.txt"; then
    gard_credentials=`cat gard-credentials.txt`
    sbt stitcher/"runMain ncats.stitcher.impl.GARDEntityFactory\$Register $out $gard_credentials"
fi

#load GHR
sbt $opts stitcher/"runMain ncats.stitcher.impl.GHREntityFactory $out"

#load NORD
sbt $opts stitcher/"runMain ncats.stitcher.impl.NORDEntityFactory $out"

# load ontologies
#sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $owl_files"
for f in $owl_files; do
    sbt $opts -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $f"
done

# hit omim api to get additional data not in ontology
#if test -f "omim-credentials.txt"; then
#    omim_credentials=`cat omim-credentials.txt`
#    sbt stitcher/"runMain ncats.stitcher.impl.OMIMUpdateEntityFactory $out $omim_credentials"
#fi

#load ChEBI
sbt $opts -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $cache $owl_path/chebi.xrdf.gz"

#load rancho
sbt $opts stitcher/"runMain ncats.stitcher.impl.InxightEntityFactory $out $cache data/rancho-disease-drug_2018-12-18_13-30.txt"

# load orphan designation
sbt $opts stitcher/"runMain ncats.stitcher.impl.FDAOrphanDesignationEntityFactory $out data/FDAOrphanGARD_20190216.txt"

#load hpo annotations
if test -e $hpo/phenotype.hpoa; then
    sbt stitcher/"runMain ncats.stitcher.impl.HPOEntityFactory $out $hpo/phenotype.hpoa"
fi

#load additional orphanet relationships if available
#if test -d $orphclass; then
#    sbt stitcher/"runMain ncats.stitcher.impl.OrphanetClassificationEntityFactory $out $orphclass"
#fi

if test -e $orpha/en_product9_prev.xml; then
    sbt stitcher/"runMain ncats.stitcher.impl.OrphanetPrevalenceEntityFactory $out $orpha/en_product9_prev.xml"
fi

if test -e $orpha/en_product4_HPO.xml; then
    sbt stitcher/"runMain ncats.stitcher.impl.OrphanetHPOEntityFactory $out $orpha/en_product4_HPO.xml"
fi

# load disease-gene association; the associations in the owl file aren't up to date
if test -e $orpha/en_product6.xml; then
    sbt stitcher/"runMain ncats.stitcher.impl.OrphanetDiseaseGeneAssociationEntityFactory $out $orpha/en_product6.xml"
fi

#load MedGen if available
if test -d $medgen; then
    sbt $opts stitcher/"runMain ncats.stitcher.impl.MedGenEntityFactory $out $medgen"
fi

#load clinvar if avaiable
if test -f $clinvar; then
    sbt $opts stitcher/"runMain ncats.stitcher.impl.ClinVarVariationEntityFactory $out $clinvar"
fi

#load PPI if available
if test -f $ppi; then
    sbt $opts stitcher/"runMain ncats.stitcher.impl.PPIEntityFactory $out $ppi"
fi

# make sure these are loaded after medgen
owl_last="efo.owl.gz mondo.owl.gz"
for f in `echo $owl_last | xargs printf " ${owl_path}/%s"`; do
    sbt $opts -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory $out $f"
done
