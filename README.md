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
    1. Make sure you have a file `.sbtopts` in your `stitcher` directory that has the following content:
        ```console
        -J-Xms1024M -J-Xmx16G -J-Xss1024M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC
        ```

    2. From the `stitcher` directory, run:
        ```console
        $ ./scripts/stitching/stitch-all-current.sh
        ```
        The script will create a date- and time-stamped database named according to the following convention `stitchvYYYYMMDD-hhmmss.db`.  
        **NOTE:** Building the database and stitching should take about 14 hours total on a server with two Intel(R) Xeon(R) E5-2665 CPUs. The application uses about 20GB of RAM.

    3. Alternatively, to create a log file, run:
        ```console
        $ ./scripts/stitching/master-stitch-all-current.sh
        ```
        **NOTE:** Since the process takes a while, it's better run the process in a separate `screen` to keep the process running, if the connection to the server/terminal is reset.
        While `nohup` is another option, it is problematic in this case, as it will stop the job at the end of every command due to a `tty` output attempt. 
        ```console
        $ screen
        $ ./scripts/stitching/master-stitch-all-current.sh
        #press 'ctrl+a', then 'd' to disconnect from the screen
        ```

    **NOTE:** If you encounter errors, try cleaning the project by removing all target directories directly, and then re-run the script:
    ```console
    $ find . -name target -type d -exec rm -rf {} \;
    $ bash scripts/stitch-all-current.sh
    ```

## Testing Locally
### Stitching (Inxight)
Since the stitching takes a long time, one might want to test a small subset of substances.  
1) Prepare test data sources by selecting a desired subset of substances in each.  

2) To make a G-SRS data source, run:
    ```console
    $ ./scripts/stitcher-testing/make-test-gsrs-dump.sh UNII
    ```
    The script takes a UNII as an argument and will excise that record from the G-SRS dump and a path to that G-SRS dump.  
    **NOTE:** the first run is slow, but the follow-up runs are fast, as the script will attempt to locate temporary files it produced in `/tmp` directory.
3) Modify the test script accordingly and run it:
    ```console
    $ ./scripts/stitching/test/stitch-all-current.sh
    ```

### App Deployment
1) In your `stitcher` directory, run:
    ```console
    $ ./scripts/deployment/restart-stitcher-from-repo.sh YOUR-DATABASE-PATH
    ```
   The script takes one argument, the path to your desired database.
  
2) When prompted in the console, in your browser navigate to  
   http://localhost:9000/app/stitches/latest

## Deployment
### Build the Binary Distribution 
**NOTE:** only do this if you have changed the stitcher code or starting anew.  


0) **Please** make sure you run the following test when you update the stitching algorithm
    ```console
    sbt stitcher/"testOnly ncats.stitcher.test.TestStitcher"
    ```
    and ensure all the basic stitching test cases are passed before doing a build

1) Make a distribution. In the `stitcher` directory run:
    ```
    sbt dist
    ```
    It will be created in `stitcher/target/universal/` and have a name similar to `ncats-stitcher-master-20171110-400d1f1.zip`.

2) Copy the archive to the deployment server (e.g., `dev.ncats.io`). For example:
    ```
    #navigate to path-to-stitcher-parent-directory/stitcher/target/universal/ 
    #scp to the server
    $ scp ncats-stitcher-master-20171110-400d1f1.zip centos@dev.ncats.io:/tmp
    ```

3) Unzip into the desired folder.
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
    $ ./scripts/deployment/restart-stitcher.sh ncats-stitcher-master-20171110-400d1f1 stitchv1.db
    ```

### Summary 
#### To run a new stitcher instance you'll need:
1) A distribution folder (e.g. `~/ncats-stitcher-master-20171110-400d1f1`).
2) A database (e.g. `stitchv1.db`).
3) A `files-for-stitcher.ix` folder with three files.
4) The script for (re)starting stitcher `restart-stitcher.sh`.

Examples
===========================================
## Sample API Queries

https://stitcher.ncats.io/app/stitches/latest  
https://stitcher.ncats.io/app/stitches/latest/ + UNII  
https://stitcher.ncats.io/app/stitches/latest/aspirin  
https://stitcher.ncats.io/api/datasources  

## Scraping Inxight Target Data

The activity data is linked to the substance records using the UNII identifier.

For example, the UNII for cannabidiol is 19GBJ60SN5, and [Inxight](https://drugs.ncats.io/drug/19GBJ60SN5) contains the following data:

| Primary Target | Pharmacology | Condition | Potency |
| ---- |---- | ---- | ---- |
|Vanilloid receptor | Agonist || 3.2 µM [EC50] |
|Dopamine D2 receptor | Partial Agonist || 11.0 nM [Ki] |
|Glycine receptor subunit alpha-3 | Binding Agent |
|G-protein coupled receptor 55 | Antagonist || 445.0 nM [IC50] |
|Serotonin 1a (5-HT1a) receptor | Agonist |

The target data can be found in the json in [Stitcher](https://stitcher.ncats.io/api/stitches/v1/19GBJ60SN5) as well under:  
`sgroup / properties / targets`  
For example:
```json
"targets": 
    [
      0: 
      {
        "node": 380111
        "value": "eyJwcmltYXJ5X3RhcmdldF9pZCI6IkNIRU1CTDIxNCIsImNvbXBvdW5kX2lkIjo4MjMzLjAsInRhcmdldF9wcmltYXJ5X3RhcmdldF90eXBlIjoiQ2hFTUJMIiwicHJpbWFyeV90YXJnZXRfdXJpIjoiaHR0cHM6Ly93d3cubmNiaS5ubG0ubmloLmdvdi9wdWJtZWQvMTYyNTg4NTMiLCJ0YXJnZXRfcHJpbWFyeV9wb3RlbmN5X3R5cGUiOiJVbmtub3duIiwicHJpbWFyeV9wb3RlbmN5X3VyaSI6IlVua25vd24iLCJ0YXJnZXRfcGhhcm1hY29sb2d5IjoiQWdvbmlzdCIsInByaW1hcnlfdGFyZ2V0X2xhYmVsIjoiU2Vyb3RvbmluIDFhICg1LUhUMWEpIHJlY2VwdG9yIiwidGFyZ2V0X2lkIjo5NjA4fQ"
      }
```
The `value` properties are base64 encoded json objects and decoding them yields all relevant target information including the source URL, e.g.:

```json
{
    "primary_target_id": "CHEMBL214",
    "compound_id": 8233.0,
    "target_primary_target_type": "ChEMBL",
    "primary_target_uri": "https://www.ncbi.nlm.nih.gov/pubmed/16258853",
    "target_primary_potency_type": "Unknown",
    "primary_potency_uri": "Unknown",
    "target_pharmacology": "Agonist",
    "primary_target_label": "Serotonin 1a (5-HT1a) receptor",
    "target_id": 9608
}
```
**NOTE:** The `node` property refers to the Stitcher data source.
**NOTE:** all homonymous properties from different sources are merged into a single array in Stitcher; therefore, some `targets` coming from different sources may not be base64 encoded. 

To get the entire dataset, you can iterate over all entries in the stitcher API using 'top' (must be <11) and 'skip', e.g.:  
https://stitcher.ncats.io/api/stitches/v1?top=10&skip=590 


Troubleshooting
===================================

## Issue #1   

**Description:**    
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

The underlying Neo4j for stitcher is publicly accessible [here](https://stitcher.ncats.io/browser/).  
Please specify ```stitcher.ncats.io:80``` in the ```Host``` field.  
No credentials are needed.

Data Preparation
=========================================
Scripts for Recent Approval Data from FDA

```console
cd scripts
python approvalYears.py   # requires python 3+
```
In the `/data` folder, there should now be a file named according to the following convention: `approvalYears-YYYY-MM-DD.txt`.  
If acceptable, update the filename reference in `/data/conf/ob.conf` to point to this new file.


