


update gsrs
* run workflow in stitcher-data-inxight to update gsrs
* update all the places that name is hardcoded in this repo :(

run this workflow
* probably best to run in the docker container, so that all the java dependencies will be available

update name in ob.conf
* name = "Drugs@FDA & OB, February 2024"

update name in dailymed_summary.conf
* name = "DailyMed Rx, February 2024"



Other tools for your toolbox
* Save a png of the workflow
  * snakemake --dag | dot -T png > workflow.png