Stitcher
========

Software for the ingestion and semantic normalization of datasets. Stitcher employs entity resolution algorithms to partition entities within a given dataset into disjoint sets such that those within the same set are considered equivalent.  Thus, Stitcher is used to untangle a web of connections between entities from multiple sources, form clusters representing unique substances, and thereby locate the unified set of properties for each substance. At the last step, derived variables are computed by traversing the unified property set.

A technical description of this approach can be found in https://github.com/ncats/stitcher/tree/master/paper

Building Stitcher
========

This codebase is based on the latest version of the Play framework
and as such it needs Java 8 to build. Modules are defined under
```modules```. The main Play app is defined in ```app```. To build the
main app, type

```console
$ ./activator {target}
```

where ```{target}``` can be one of
{```compile```,```run```,```test```, ```dist```}. Building modules is
similar:

```console
$ ./activator {module}/{target}
```

where ```{module}``` is the module name as it appears under ```modules/```
and ```{target}``` can be {```compile```, ```test```}. To run a particular
class in a particular module, use the ```runMain``` syntax, e.g.,

```console
$ ./activator "project stitcher" "runMain ncats.stitcher.tools.DuctTape"
```

Stitching Approach
==================

We propose a graph-based approach to entity stitching and
resolution. Briefly, our approach uses clique detection to do the
stitching and resolution as follows:

1. For a given hypergraph (multi-edge) of stitched entities, extract
connected components based on stitching keys as defined in
```StitchKey```.

2. For each connected component, perform exhaustive clique enumeration
over each stitch key. A clique is a complete subgraph of size 3 or
larger.

3. Next we identify a set of high confidence cliques. A high
confidence clique is a clique for which its members do not belong to
any other clique. All nodes in a clique are merged to become a
stitched node.

4. For the leftover cliques, we perform a sort by descending order of
the value |V| * |E| where |V| and |E| are the clique size and the
cardinality of stitch keys, respectively. Stitched nodes are created
as we iterate through this order ignoring any nodes that have already
been stitched.

Detailed Instructions
==================

## Preparing the Database and Stitching

1) Try invoking the `sbt` shell to check if it is available, then `exit`.
```console
$ sbt
```

2) Initiate (define auxiliary functions, check for java version, etc.), then `exit`.
```console
$ bash activator2
```

3) Build, stitch, and calculate events. 
a) Make sure you have a file `.sbtopts` in your `stitcher` directory that has the following content:
```console
-J-Xms1024M -J-Xmx16G -J-Xss1024M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC
```

b) Check the script and search for the database name (e.g. `stitchv1.db`):
```console
$ cat scripts/stitch-all-current.sh
```
If you have a database with the same name in your `stitcher` directory, either remove it or modify the script to have a different db name (e.g. `stitchv2.db`).

c) From the `stitcher` directory, run:
```console
$ bash scripts/stitch-all-current.sh
```
NOTE: Building the databse and stitching should take about 4 and 5 hours, respectively, on a laptop (i5-4200U @ 2.3 GHz, 8GB RAM).
Complete process on a server (`ifxdev.ncats.nih.gov`) takes approximately 5-6 hours.

NOTE: Since the process takes a while, it's better run the process in a separate `screen` to keep the process running, if the connection to the server/terminal is reset.
While `nohup` is another option, it is problematic in this case, as it will stop the job at the end of every command due to a `tty` output attempt. 
```console
$ screen
$ bash scripts/stitch-all-current.sh > stitch.out 2>&1
#press 'ctrl+a', then 'd' to disconnect from the screen
```

NOTE: If you encounter errors, try cleaning the project by removing all target directories directly, and then re-run the script:
```console
$ find . -name target -type d -exec rm -rf {} \;
$ bash scripts/stitch-all-current.sh
```

## Testing Locally

1) In your `stitcher` directory, make a symbolic link `stitcher.ix/data.db` pointing to the database you have just made.
```console
#first, remove old link or a folder with the same name (if present)
$ rm -r stitcher.ix/data.db
#then create the symlink
$ ln -s ../stitchv1.db stitcher.ix/data.db
```

2) Navigate to your `stitcher` directory and run the project.
```console
$ sbt run
```

3) When prompted in the console, navigate to http://localhost:9000/app/stitches/latest in your browser.

## Deployment  

### Build the Binary Distribution 
####(optional -- only do this if you have changed the stitcher code or starting anew)

0) !!!*Please* make sure you run the following test when you update the stitching algorithm
```console
sbt stitcher/"testOnly ncats.stitcher.test.TestStitcher"
```
and ensure all the basic stitching test cases are passed before doing a build

1) Make a distribution. In the `stitcher` directory run:
```
sbt dist
```
It will be created in `stitcher/target/universal/` and have a name similar to `ncats-stitcher-master-20171110-400d1f1.zip`.

2) Copy the archive to the deployment server (e.g. `dev.ncats.io`). For example:
```
#navigate to path-to-stitcher-parent-directory/stitcher/target/universal/ 
#scp to the server
$ scp ncats-stitcher-master-20171110-400d1f1.zip centos@dev.ncats.io:/tmp
```

3) Unzip into the desired folder (on `centos@dev.ncats.io`, it is `~`).
```
#navigate to the desired folder on the deployment server
$ ssh centos@dev.ncats.io
#unzip
$ unzip /tmp/ncats-stitcher-master-20171110-400d1f1.zip
```

### Deploy

1) In the `stitcher` folder (where you have prepared the database), archive the database folder and copy it over to the deployment server.
```
$ zip -r stitchv1db.zip stitchv1.db/
$ scp stitchv1db.zip centos@dev.ncats.io:/tmp
```

2) On the deployment server, navigate to a directory containing the stitcher distribution folder and unzip the database.
```
$ ssh centos@dev.ncats.io
$ unzip /tmp/stitchv1db.zip
```

3) Start up the app. The script takes the distribution and db folders as arguments.
```
$ bash restart-stitcher.sh ncats-stitcher-master-20171110-400d1f1 stitchv1.db
```

### Summary 
#### To run a new stitcher instance you'll need in the same directory
1) A distribution folder (e.g. `~/ncats-stitcher-master-20171110-400d1f1`).
2) A database (e.g. `~/stitchv1.db`).
3) A `files-for-stitcher.ix` folder with three files.
4) The script for (re)starting stitcher `restart-stitcher.sh`.

## Useful links

https://stitcher.ncats.io/app/stitches/latest  
https://stitcher.ncats.io/app/stitches/latest/ + UNII  
https://stitcher.ncats.io/app/stitches/latest/aspirin  
https://stitcher.ncats.io/api/datasources  

## Scraping Inxight target data

The activity data is linked to the substance records using the UNII identifier.

For example, the UNII for cannabidiol is 19GBJ60SN5 
https://drugs.ncats.io/drug/19GBJ60SN5

| Primary Target | Pharmacology | Condition | Potency |
| ---- |---- | ---- | ---- |
|Vanilloid receptor | Agonist || 3.2 µM [EC50] |
|Dopamine D2 receptor | Partial Agonist || 11.0 nM [Ki] |
|Glycine receptor subunit alpha-3 | Binding Agent |
|G-protein coupled receptor 55 | Antagonist || 445.0 nM [IC50] |
|Serotonin 1a (5-HT1a) receptor | Agonist |

The target data can be found in the json at:

https://stitcher.ncats.io/api/stitches/v1/19GBJ60SN5

 -> sgroup / properties / Targets

```json
"Targets": [
    {
  "node": 345357,
  "value": "eyJQcmltYXJ5UG90ZW5jeVR5cGUiOiJVbmtub3duIiwiUHJpbWFyeVRhcmdldFR5cGUiOiJDaEVNQkwiLCJQcmltYXJ5VGFyZ2V0VXJpIjoiaHR0cHM6Ly93d3cubmNiaS5ubG0ubmloLmdvdi9wdWJtZWQvMTYyNTg4NTMiLCJQcmltYXJ5UG90ZW5jeVVyaSI6IlVua25vd24iLCJQcmltYXJ5VGFyZ2V0SWQiOiJDSEVNQkwyMTQiLCJUYXJnZXRQaGFybWFjb2xvZ3kiOiJBZ29uaXN0IiwiaWQiOiJmZDk0MjAzZDIxIiwiUHJpbWFyeVRhcmdldExhYmVsIjoiU2Vyb3RvbmluIDFhICg1LUhUMWEpIHJlY2VwdG9yIn0="
},
    {
  "node": 345357,
  "value": "eyJQcmltYXJ5UG90ZW5jeVR5cGUiOiJVbmtub3duIiwiUHJpbWFyeVRhcmdldFR5cGUiOiJDaEVNQkwiLCJQcmltYXJ5VGFyZ2V0VXJpIjoiaHR0cHM6Ly93d3cubmNiaS5ubG0ubmloLmdvdi9wdWJtZWQvMjI1ODU3MzYiLCJQcmltYXJ5UG90ZW5jeVVyaSI6IlVua25vd24iLCJQcmltYXJ5VGFyZ2V0SWQiOiJDSEVNQkwxMDc1MDkyIiwiVGFyZ2V0UGhhcm1hY29sb2d5IjoiQmluZGluZyBBZ2VudCIsImlkIjoiZDY4ZGMxMzNkMSIsIlByaW1hcnlUYXJnZXRMYWJlbCI6IkdseWNpbmUgcmVjZXB0b3Igc3VidW5pdCBhbHBoYS0zIn0="
},
    {
  "node": 345357,
  "value": "eyJQcmltYXJ5UG90ZW5jeVR5cGUiOiJFQzUwIiwiUHJpbWFyeVBvdGVuY3lWYWx1ZSI6IjMuMiIsIlByaW1hcnlUYXJnZXRUeXBlIjoiQ2hFTUJMIiwiUHJpbWFyeVRhcmdldFVyaSI6Imh0dHBzOi8vd3d3Lm5jYmkubmxtLm5paC5nb3YvcHVibWVkLzExNjA2MzI1IiwiUHJpbWFyeVBvdGVuY3lVcmkiOiJodHRwczovL3d3dy5uY2JpLm5sbS5uaWguZ292L3B1Ym1lZC8xMTYwNjMyNSIsIlByaW1hcnlQb3RlbmN5RGltZW5zaW9ucyI6IsK1TSIsIlByaW1hcnlUYXJnZXRJZCI6IkNIRU1CTDQ3OTQiLCJUYXJnZXRQaGFybWFjb2xvZ3kiOiJBZ29uaXN0IiwiaWQiOiI0Y2RiMTc0ZTZmIiwiUHJpbWFyeVRhcmdldExhYmVsIjoiVmFuaWxsb2lkIHJlY2VwdG9yIn0="
},
    {
  "node": 345357,
  "value": "eyJQcmltYXJ5UG90ZW5jeUNvbW1lbnQiOiJjYW5uYWJpZGlvbCBpbmhpYml0ZWQgdGhlIGJpbmRpbmcgb2YgcmFkaW8tZG9tcGVyaWRvbmUgd2l0aCBkaXNzb2NpYXRpb24gY29uc3RhbnRzIG9mIDEx4oCJbk0gYXQgZG9wYW1pbmUgRDJIaWdoIHJlY2VwdG9ycyBhbmQgMjgwMOKAiW5NIGF0IGRvcGFtaW5lIEQyTG93IHJlY2VwdG9ycywgaW4gdGhlIHNhbWUgYmlwaGFzaWMgbWFubmVyIGFzIGEgZG9wYW1pbmUgcGFydGlhbCBhZ29uaXN0IGFudGlwc3ljaG90aWMgZHJ1ZyBzdWNoIGFzIGFyaXBpcHJhem9sZS4iLCJQcmltYXJ5UG90ZW5jeVR5cGUiOiJLaSIsIlByaW1hcnlQb3RlbmN5VmFsdWUiOiIxMS4wIiwiUHJpbWFyeVRhcmdldFR5cGUiOiJDaEVNQkwiLCJQcmltYXJ5VGFyZ2V0VXJpIjoiaHR0cHM6Ly93d3cubmNiaS5ubG0ubmloLmdvdi9wdWJtZWQvMjc3NTQ0ODAiLCJQcmltYXJ5UG90ZW5jeVVyaSI6Imh0dHBzOi8vd3d3Lm5jYmkubmxtLm5paC5nb3YvcHVibWVkLzI3NzU0NDgwIiwiUHJpbWFyeVBvdGVuY3lEaW1lbnNpb25zIjoibk0iLCJQcmltYXJ5VGFyZ2V0SWQiOiJDSEVNQkwyMTciLCJUYXJnZXRQaGFybWFjb2xvZ3kiOiJQYXJ0aWFsIEFnb25pc3QiLCJpZCI6IjY0NzM4NmRlZjYiLCJQcmltYXJ5VGFyZ2V0TGFiZWwiOiJEb3BhbWluZSBEMiByZWNlcHRvciJ9"
},
    {
  "node": 345357,
  "value": "eyJQcmltYXJ5UG90ZW5jeVR5cGUiOiJJQzUwIiwiUHJpbWFyeVBvdGVuY3lWYWx1ZSI6IjQ0NS4wIiwiUHJpbWFyeVRhcmdldFR5cGUiOiJDaEVNQkwiLCJQcmltYXJ5VGFyZ2V0VXJpIjoiaHR0cHM6Ly93d3cubmNiaS5ubG0ubmloLmdvdi9wdWJtZWQvMTc4NzYzMDIiLCJQcmltYXJ5UG90ZW5jeVVyaSI6Imh0dHBzOi8vd3d3Lm5jYmkubmxtLm5paC5nb3YvcHVibWVkLzE3ODc2MzAyIiwiUHJpbWFyeVBvdGVuY3lEaW1lbnNpb25zIjoibk0iLCJQcmltYXJ5VGFyZ2V0SWQiOiJDSEVNQkwxMDc1MzIyIiwiVGFyZ2V0UGhhcm1hY29sb2d5IjoiQW50YWdvbmlzdCIsImlkIjoiZmExZTU0ZGM4MyIsIlByaW1hcnlUYXJnZXRMYWJlbCI6IkctcHJvdGVpbiBjb3VwbGVkIHJlY2VwdG9yIDU1In0="
}
]
```

The 'value's are base64 encoded strings ... decoding them gives you json with the actual target text and URLs from the webpage, e.g.:

```json
{"PrimaryPotencyType":"Unknown","PrimaryTargetType":"ChEMBL","PrimaryTargetUri":"https://www.ncbi.nlm.nih.gov/pubmed/16258853","PrimaryPotencyUri":"Unknown","PrimaryTargetId":"CHEMBL214","TargetPharmacology":"Agonist","id":"fd94203d21","PrimaryTargetLabel":"Serotonin 1a (5-HT1a) receptor"}
```

To get the entire dataset, you can iterate over all entries in the stitcher API using 'top' and 'skip' for example:

https://stitcher.ncats.io/api/stitches/v1?top=10&skip=590 


## Troubleshooting
- **Problem:** 
    ```
    java.lang.NumberFormatException: For input string: "0x100"
    ```
    **Cause:**    
    `SBT` uses `jline` for terminal output. The latter in turn uses the `infocmp` utility provided by `ncurses`, which expects only decimal values. This behaviour was fixed in a new version of `jline` and and newer version of `SBT`, however version `0.13.15` used for this project still suffers from it.  
    **Solution:**   
    Add the following to your `~/.bashrc`:  
    ```
    export TERM=xterm-color
    ```

Access to underlying Neo4j database
===================================

The underlying Neo4j for stitcher is publicly accessible [here](https://stitcher.ncats.io/browser/). Please specify ```stitcher.ncats.io:80``` in the ```Host``` field. No credentials are needed.

Scripts for Recent Approval Data from FDA
=========================================

cd scripts
python approvalYears.py   [requires python 3+]
in the /data folder, there should now be a file like approvalYears-2020-12-14.txt. If acceptable, update the filename reference in /data/conf/ob.conf to point to this new file.

