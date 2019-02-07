#!/usr/bin/env python

import os
import sys
import cookielib
import urllib
import urllib2
import json
import time
import argparse
import pandas as pd

# check that the python version is correct (need 2)
if sys.version_info[0] > 2:
    raise "Must be using Python 2! Aborting."

# check for arguments
args_p = argparse.ArgumentParser(description="Run Some Stitcher Tests")
args_p.add_argument('addr',
                    help="""a full Stitcher address OR
                            a shorthand: 'prod', 'dev', or 'local'""")
args_p.add_argument('--unii',
                    nargs="?",
                    default="../../temp/UNIIs-2018-09-07/UNII Names 31Aug2018.txt",
                    help="path to a file with unii names")

args_p.add_argument("--appyears",
                    nargs="?",
                    default="../../data/approvalYears.txt",
                    help="path to a file with unii names")

args_p.add_argument("--fdanme",
                    nargs="?",
                    default="../../data/FDA-NMEs-2018-08-07.txt",
                    help="path to a file with unii names")

site_arg = args_p.parse_args().addr
unii_path = args_p.parse_args().unii
appyears_path = args_p.parse_args().appyears
fdanme_path = args_p.parse_args().fdanme

switcher = {
    "prod": "https://stitcher.ncats.io/",
    "dev": "https://stitcher-dev.ncats.io/",
    "local": "http://localhost:8080/"
    }

if site_arg in switcher:
    site = switcher[site_arg]
else:
    site = site_arg

date = time.strftime("%Y-%m-%d", time.gmtime())

cookies = cookielib.CookieJar()

opener = urllib2.build_opener(
    urllib2.HTTPRedirectHandler(),
    urllib2.HTTPHandler(debuglevel=0),
    urllib2.HTTPSHandler(debuglevel=0),
    urllib2.HTTPCookieProcessor(cookies))
opener.addheaders = [
    ('User-agent', ('Mozilla/4.0 (compatible; MSIE 6.0; '
                    'Windows NT 5.2; .NET CLR 1.1.4322)'))
]


def requestJson(uri):
    try:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()
        obj = json.loads(response)
        return obj
    except:
        sys.stderr.write("failed: "+uri+"\n")
        sys.stderr.flush()
        time.sleep(5)


def uniiClashes(unii2stitch, stitch):
    '''  defined in main
    header = ["UNII -- UNII PT"]
    '''

    for node in stitch['sgroup']['members']:
        if "stitches" in node and "I_UNII" in node["stitches"]:
            uniis = node['stitches']['I_UNII']

            if not isinstance(uniis, list):
                uniis = [uniis]

            for unii in uniis:
                unii2stitch.setdefault(unii,
                                       [" -- ".join([unii,
                                                     all_uniis.get(unii, "")])
                                        ])
                if stitch['id'] not in unii2stitch[unii]:
                    unii2stitch[unii].append(stitch['id'])

    return unii2stitch


def approvedStitches(approved, stitch):
    ''' defined in main
    header = ["UNII -- UNII PT",
              "Approval Year",
              "Stitch",
              "Rank"]
    '''

    if stitch['approved']:
        apprYear = "not given"

        if 'approvedYear' in stitch:
            apprYear = stitch['approvedYear']

        parent = stitch['sgroup']['parent']
        rank = stitch['rank']
        unii = ''

        for node in stitch['sgroup']['members']:
            if node['node'] == parent:
                if 'id' not in node:
                    unii = node['name']
                else:
                    unii = node['id']

        name = getName(stitch)

        approved[unii] = [" -- ".join([unii,
                                       all_uniis.get(unii, "")]),
                          apprYear,
                          parent,
                          rank]

    return approved


def nmeStitches(stitch2nmes, stitch, nmelist):
    ''' defined in main
    header = ["UNII -- UNII PT",  # can repeat
              "Stitch",
              "Rank"]
    '''

    key = stitch['id']
    entries = []

    for node in stitch['sgroup']['members']:
        if node['source'] == 'G-SRS, July 2018':
            if node['id'] in nmelist:
                # print node['id']
                # add the UNII and its preferred name to the list
                entries.append(" -- ".join([node['id'],
                                            all_uniis.get(node['id'], "")]))

    # print entries
    if len(entries) > 1:
        entries.sort()
        entries.insert(1, key)
        entries.insert(2, stitch['rank'])
        stitch2nmes[entries[0]] = entries

    return stitch2nmes


NMEs = []
NMEs2 = []


def nmeClashes(stitch2nmes, stitch):
    return nmeStitches(stitch2nmes, stitch, NMEs)


def nmeClashes2(stitch2nmes, stitch):
    return nmeStitches(stitch2nmes, stitch, NMEs2)


def PMEClashes(stitch2pmes, stitch):
    '''  defined in main
    header = ["PME Entry",
              "Stitch",
              "Rank"]
    '''

    key = stitch['id']
    entries = []

    for node in stitch['sgroup']['members']:
        if node['source'] == ("Pharmaceutical Manufacturing Encyclopedia "
                              "(Third Edition)"):
            entries.append(node['name'])

    # if more than one PME entry found,
    # sort and add stitch info and rank after the first one
    if len(entries) > 1:
        entries.sort()
        entries.insert(1, key)
        entries.insert(2, stitch['rank'])
        stitch2pmes[entries[0]] = entries

    return stitch2pmes


def activemoietyClashes(stitch2ams, stitch):
    key = stitch['id']
    entries = []

    for node in stitch['sgroup']['members']:
        if node['source'] == 'G-SRS, July 2018':
            if 'T_ActiveMoiety' in node['stitches']:
                item = node['stitches']['T_ActiveMoiety']

                # check if item is a list -- if not, turn into a list
                if not isinstance(item, list):
                    item = [item]

                # if length of the t_activemoiety list is longer than 1, ignore
                if len(item) > 1:
                    sys.stderr.write("ignoring multiple active moieties"
                                     " for GSRS entry\n")

                # get a preferred name for the active moiety unii
                item = " -- ".join([item[0],
                                    all_uniis.get(item[0], "")])

                # if not already recoreded, record
                if item not in entries:
                    entries.append(item)

    if len(entries) > 1:
        entries.sort()
        entries.insert(1, key)
        entries.insert(2, stitch['rank'])
        stitch2ams[entries[0]] = entries

    return stitch2ams


orphanList = ['Pharmaceutical Manufacturing Encyclopedia (Third Edition)',
              'Broad Institute Drug List 2017-03-27',
              'Rancho BioSciences, August 2018',
              'DrugBank, July 2018',
              'NCATS Pharmaceutical Collection, April 2012',
              'Withdrawn and Shortage Drugs List Feb 2018']


def findOrphans(orphans, stitch):
    '''  defined in main
    header = ["Name / UNII / ID",
              "Blank / UNII PT",
              "Source"]
    '''

    key = stitch['id']
    rank = stitch['rank']

    if rank == 1:
        node = stitch['sgroup']['members'][0]

        if node['source'] in orphanList:
            name = ''
            id = ''
            status = ''

            if 'id' in node:
                id = node['id']
            else:
                id = node['name']
            if 'name' in node:
                name = node['name']

            if node['source'] == 'Broad Institute Drug List 2017-03-27':
                if 'clinical_phase' in stitch['sgroup']['properties']:
                    status = '|' + stitch['sgroup']['properties']['clinical_phase']['value']

            if node['source'] == 'DrugBank, July 2018':
                if 'groups' in stitch['sgroup']['properties']:
                    status = ''

                    for group in stitch['sgroup']['properties']['groups']:
                        status = status + '|' + group['value']

            if node['source'] == 'NCATS Pharmaceutical Collection, April 2012':
                if 'DATASET' in stitch['sgroup']['properties']:
                    sets = []

                    for group in stitch['sgroup']['properties']['DATASET']:
                        sets.append(group['value'])
                    sets.sort()
                    status = '|'.join(sets)

                if 'name' in stitch['sgroup']['properties']:
                    name = stitch['sgroup']['properties']['name']['value']

            if node['source'] == 'Rancho BioSciences, August 2018':
                if 'Conditions' in stitch['sgroup']['properties']:
                    status = '|has_conditions'

            item = node['source'] + status + "\t" + id
            entry = [id, all_uniis.get(id, ""), node['source'], status, name]
            orphans[item] = entry

    return orphans


def iterateStitches(funcs):
    dicts = [{} for d in range(len(funcs))]

    skip = 0
    top = 10
    max_subs = 300000

    while skip < max_subs:
        uri = site+'api/stitches/v1?top='+str(top)+'&skip='+str(skip)
        obj = requestJson(uri)

        if 'contents' not in obj:
            obj = {'contents': obj}
            skip = max_subs

        if len(obj['contents']) == 0:
            skip = max_subs
        elif obj is not None:
            for stitch in obj['contents']:
                for i in range(len(funcs)):
                    funcs[i](dicts[i], stitch)

        sys.stderr.write(uri+"\n")
        sys.stderr.flush()

        skip = skip + top

    return dicts


def getName(obj):
    name = ""
    for member in obj["sgroup"]["members"]:
        if name == "":
            if "name" in member:
                name = member["name"]
            elif "N_Name" in member["stitches"]:
                if len(member["stitches"]["N_Name"][0]) == 1:
                    name = str(member["stitches"]["N_Name"])
                else:
                    name = str(member["stitches"]["N_Name"][0])
    if name == "":
        if "Synonyms" in obj["sgroup"]["properties"]:
            if not isinstance(obj["sgroup"]["properties"]["Synonyms"],
                              (list, tuple)):
                name = obj["sgroup"]["properties"]["Synonyms"]["value"]
            else:
                name = obj["sgroup"]["properties"]["Synonyms"][0]["value"]
    if name == "":
        if "unii" in obj["sgroup"]["properties"]:
            name = obj["sgroup"]["properties"]["unii"][0]["value"]
    return name


def getEdges(nodes, edges=[], nodesrc=dict()):
    while len(nodes) > 0:
        newnodes = []
        for item in nodes:
            uri = site + "api/node/" + str(item)
            obj = requestJson(uri)
            nodesrc[item] = obj["datasource"]["name"]
            if "neighbors" in obj:
                for entry in obj["neighbors"]:
                    edges.append([item, entry["id"],
                                  entry["reltype"],
                                  entry["value"]])
                    if entry["id"] not in nodes and entry["id"] not in nodesrc:
                        newnodes.append(entry["id"])
        nodes = newnodes
    return edges, nodesrc


def getPathsFromStitch(stitch):
    paths = []
    path = [stitch[0]]
    paths.append(path)
    for item in stitch[1:]:
        npath = list(path)
        npath.append(item)
        paths.append(npath)
    return paths


def extendPaths(paths, edges):
    npaths = []
    for path in paths:
        for edge in edges:
            if edge[0] == path[-1] and edge[1] not in path:
                npath = list(path)
                npath.append(edge[1])
                npaths.append(npath)
    return npaths


def probeUniiClash():
    uri = site + 'api/stitches/latest/ZY81Z83H0X'
    obj = requestJson(uri)
    stitch = []
    stitch.append(obj['sgroup']['parent'])
    for member in obj['sgroup']['members']:
        if member['node'] not in stitch:
            stitch.append(member['node'])

    edges = []
    sp = dict()
    nodes = []
    for item in stitch:
        uri = site + 'api/node/' + str(item)
        obj = requestJson(uri)
        sp[item] = obj['parent']
        nodes.append(sp[item])
        edges.append([item, sp[item], 'parent', 'parent'])

    edges, nodesrc = getEdges(nodes, edges)

    uri = site + 'api/stitches/latest/1222096'
    obj = requestJson(uri)
    ostitch = []
    ostitch.append(obj['sgroup']['parent'])
    for member in obj['sgroup']['members']:
        if member['node'] not in ostitch:
            ostitch.append(member['node'])

    nodes = []
    for item in ostitch:
        uri = site + 'api/node/' + str(item)
        obj = requestJson(uri)
        sp[item] = obj['parent']
        nodes.append(sp[item])
        edges.append([sp[item], item, 'rparent', 'rparent'])

    edges, nodesrc = getEdges(nodes, edges, nodesrc)

    print stitch
    print ostitch
    print nodesrc
    print edges

    paths = getPathsFromStitch(stitch)
    match = []
    round = 0
    while len(match) == 0:
        for p in paths:
            if p[-1] == ostitch[-1]:
                match.append(list(p))
        if len(match) == 0:
            round = round + 1
            paths = extendPaths(paths, edges)
            if len(paths) == 0 or round > 10:
                match = ["impossible"]
    print match


def get_uniis(unii_path):
    uniis = pd.read_csv(unii_path,
                        sep="\t")
    return (uniis[["UNII", "Display Name"]].drop_duplicates()
                                           .set_index("UNII")
                                           .to_dict()["Display Name"])


def output2df(output, test_name, header):
    # turn the dict with results into
    output_df = pd.DataFrame.from_dict(output,
                                       orient="index")

    if len(output_df.index) < 1:
        return output_df

    # add appropriate header
    n_extra_cols = output_df.shape[1] - len(header)

    if test_name == "uniiClashes":
        header += ["Stitch"]*n_extra_cols
    elif (test_name.startswith("nmeClashes")
          or test_name == "activemoietyClashes"):
        header += ["UNII -- UNII PT [OTHER]"]*n_extra_cols
    elif test_name == "PMEClashes":
        header += ["PME Entry [OTHER]"]*n_extra_cols
    else:
        header += ["UNKNOWN COLUMN"]*n_extra_cols

    output_df.columns = header

    # insert the test name
    output_df.insert(0, "Test", test_name)

    return output_df


if __name__ == "__main__":

    # list of tests to perform
    # nmeClashes and nmeClashes2 => run nmeStitches with different NME lists
    # from ApprovalYears.txt and FDA-NMEs-2018-08-07.txt, respectively
    # nmeStitches: Multiple NME approvals that belong to a single stitch
    # PMEClashes: Multiple PME entries that belong to a single stitch
    # activemoietyClashes: Multiple GSRS active moieties listed
    # for a single stitch
    # uniiClashes: Different stitches (listed by id)
    # that share a UNII - candidates for merge
    # findOrphans: Some resource entries were supposed to be stitched,
    # but are orphaned
    # approvedStitches: Report on all the approved stitches from API

    tests = [nmeClashes,
             nmeClashes2,
             PMEClashes,
             activemoietyClashes,
             uniiClashes,
             findOrphans,
             approvedStitches]

    headers = {"nmeClashes": ["UNII -- UNII PT",
                              "Stitch",
                              "Rank"],
               "nmeClashes2": ["UNII -- UNII PT",
                               "Stitch",
                               "Rank"],
               "PMEClashes": ["PME Entry",
                              "Stitch",
                              "Rank"],
               "activemoietyClashes": ["UNII -- UNII PT",
                                       "Stitch",
                                       "Rank"],
               "uniiClashes": ["UNII -- UNII PT"],
               "findOrphans": ["Name / UNII / ID",
                               "Blank / UNII PT",
                               "Source",
                               "Status",
                               "Name"],
               "approvedStitches": ["UNII -- UNII PT",
                                    "Approval Year",
                                    "Stitch",
                                    "Rank"]}

    # initialize list of NMEs
    nmeList = open(appyears_path, "r").readlines()
    for entry in nmeList[1:]:
        sline = entry.split('\t')
        if sline[0] not in NMEs:
            NMEs.append(sline[0])

    nmeList2 = open(fdanme_path, "r").readlines()
    for entry in nmeList2[1:]:
        sline = entry.split('\t')
        if sline[3] not in NMEs2:
            NMEs2.append(sline[3])

    # initialize UNII preferred terms
    all_uniis = get_uniis(unii_path)

    # iterate over stitches, perform tests
    # returns a list of dictionaries
    outputs = iterateStitches(tests)

    # remove unimportant output for uniiClashes
    if uniiClashes in tests:
        outputs[tests.index(uniiClashes)] = {
                k: v for k,
                v in outputs[tests.index(uniiClashes)].items()
                if len(v) > 3
            }

    xl_writer = pd.ExcelWriter("_".join([date,
                                         "regression",
                                         site_arg,
                                         ".xlsx"]))

    for test_index in range(len(tests)):
        test_name = tests[test_index].__name__

        '''
        print test_name
        print test_index
        print outputs[test_index]
        '''

        output_df = output2df(outputs[test_index],
                              test_name,
                              headers[test_name])

        output_df.to_excel(xl_writer,
                           sheet_name=test_name,
                           header=True,
                           index=False)

    xl_writer.save()

'''
    # TODO post-process unii clashes if desired
    for line in open("unii-clash.txt", "r").readlines():
        sline = line[0:-1].split("\t")
        unii = sline[0]
        nodes = sline[1:]

        if len(nodes) == 2:
            uri = site + 'api/stitches/latest/' + nodes[0]
            obj1 = requestJson(uri)
            uri = site + 'api/stitches/latest/' + nodes[1]
            obj2 = requestJson(uri)
            name1 = getName(obj1).encode('ascii', 'ignore')
            name2 = getName(obj2).encode('ascii', 'ignore')
            if obj1['rank'] == 1:
                print unii, nodes[0], name1, nodes[1], obj2['rank'], name2
            elif obj2['rank'] == 1:
                print unii, nodes[1], name2, nodes[0], obj1['rank'], name1
            sys.stdout.flush()
'''
