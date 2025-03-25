
update gsrs
* run workflow in stitcher-data-inxight to update gsrs

run these workflow
* probably best to run in the docker container, so that all the java dependencies will be available
* update data files
  * snakemake -s update_files/Snakefile --cores=all
* build stitcher
  * snakemake -s build_stitcher/Snakefile --cores=all

Other tools for your toolbox
* Save a png of the workflow
  * snakemake -s update_files/Snakefile --dag | dot -T png > workflow.png
