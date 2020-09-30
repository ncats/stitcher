#!/usr/bin/env python

import os
import sys
import cookielib
import urllib
import urllib2
import ssl
import json
import time
import argparse

site = "http://localhost:8080/"

cookies = cookielib.CookieJar()

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

opener = urllib2.build_opener(
    urllib2.HTTPRedirectHandler(),
    urllib2.HTTPHandler(debuglevel=0),
    urllib2.HTTPSHandler(debuglevel=0, context=ctx),
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

def applyCuration(sline):
    obj = json.loads(sline[-1])
    url = site[:-1]+obj['_uri']
    for key in obj.keys():
        if key[0] == "_": # do not post parameters created by API
            del obj[key]
    #print url, json.dumps(obj)

    req = urllib2.Request(url, json.dumps(obj), {'Content-Type': 'application/json'})
    try:
        html = urllib2.urlopen(req, context=ctx)
        sys.stderr.write(url+"\n")
        resp = html.read()
        sys.stderr.write(resp)
        sys.stderr.write("\n")
        r = json.loads(resp)
        return r
    except urllib2.HTTPError, e:
        err = 'HTTP Error ERROR en el listado code => %s \n URL=> %s\n' % (e.code,url)
        sys.stderr.write(err)
        sys.exit()
    except urllib2.URLError, e:
        err = 'URL Error ERROR en el listado reason => %s \n URL=> %s\n' % (e.reason,url)
        sys.stderr.write(err)
        sys.exit()
    return

def oldMain():

    # check that the python version is correct (need 2)
    if sys.version_info[0] > 2:
        sys.stderr.write("Must be using Python 2! Aborting.\n")
        sys.exit(1)

    # check for arguments
    args_p = argparse.ArgumentParser(description="Run Some Stitcher Tests")
    args_p.add_argument('addr',
                        help="""a full Stitcher address OR
                                a shorthand: 'prod', 'dev', 'test' or 'local'""")

    args_p.add_argument('--filename',
                        default="dbCurations.txt",
                        help="name of the file with curations to apply to stitcher database")

    site_arg = args_p.parse_args().addr
    filename = args_p.parse_args().filename

    switcher = {
        "prod": "https://stitcher.ncats.io/",
        "dev": "https://stitcher-dev.ncats.io/",
        "test": "https://stitcher-test.ncats.io/",
        "local": "http://localhost:8080/"
    }

    if site_arg in switcher:
        site = switcher[site_arg]
    else:
        site = site_arg

    fp = open(filename, "r")

    line = fp.readline()

    while line != "":
        applyCuration(line.split('\t'))
        line = fp.readline()
    fp.close()

example = """{"deltaId":1408,"nodeId":"KVS","nodeSource":"NCATS Pharmaceutical Collection, April 2012","node":368958,"operation":"add","jsonPath":"$['properties'][?(@['key']=='Synonyms' )]['value']","oldValue":null,"key":null,"value":"(R)-4-((E)-3-PHOSPHONOPROP-2-ENYL)PIPERAZINE-2-CARBOXYLIC ACID","stitches":"KVS","parentDataType":"object","dataType":"array","arrayIndex":null,"editor":"curator4 ","userName":"curator4","status":"approved","curationTimestamp":"12/15/2018","_ver":"1","_stitch":"KVS","_uri":"/api/stitches/latest/KVS/@update","_timestamp":1550001945101}"""

def getCuration(node, type, oldValue, value):
    cur = dict()
    operation = 'replace'
    if value is None or value == '':
        operation = 'remove'
    elif oldValue is None or oldValue == '':
        operation = 'add'

    cur['stitches'] = node
    cur['nodeSource'] = "DrugBank, December 2018"
    cur['nodeId'] = node
    cur['node'] = 368958
    cur['jsonPath'] = "$['properties'][?(@['key']=='"+type+"' )]['value']"
    cur['operation'] = operation
    cur['oldValue'] = oldValue
    cur['value'] = value
    cur['_uri'] = "/api/stitches/latest/" + node + "/@testupdate"

    #cur['key'] = None
    #cur['parentDataType'] = "object"
    #cur['dataType'] = "array"
    #cur['arrayIndex'] = None
    #cur['editor'] = "curator4 "
    #cur['userName'] = "curator4"
    #cur['status'] = "approved"
    #cur['curationTimestamp'] = "12/15/2018"
    return [json.dumps(cur)]

if __name__=="__main__":

    uniis = dict()
    names = dict()
    for i in range(100, 150):
        dbid = "DB%05d" % (i)
        url = "http://localhost:8080/api/stitches/latest/" + dbid
        print url
        drug = requestJson(url)
        if drug is not None:
            for member in drug['sgroup']['members']:
                if member['source'] == "DrugBank, December 2018" and member['id'] == dbid:
                    if 'stitches' in member:
                        if 'I_UNII' in member['stitches']:
                            if isinstance(member['stitches']['I_UNII'], list):
                                for unii in member['stitches']['I_UNII']:
                                    uniis[dbid] = unii
                            else:
                                uniis[dbid] = member['stitches']['I_UNII']
                        if 'N_Name' in member['stitches']:
                            if isinstance(member['stitches']['N_Name'], list):
                                for name in member['stitches']['N_Name']:
                                    names[dbid] = name
                            else:
                                names[dbid] = member['stitches']['N_Name']

    resps = dict()
    errors = []
    for unii1 in uniis.keys():
        for unii2 in uniis.keys():
            if unii1 != unii2 and uniis[unii1] != uniis[unii2]:
                type = 'unii'
                oldValue = uniis[unii1]
                value = uniis[unii2]
                curr = getCuration(unii1, type, oldValue, value)
                resp = applyCuration(curr)
                if 'statusMessage' in resp:
                    if resp['status'] == 'success':
                        if resp['statusMessage'] not in resps:
                            resps[resp['statusMessage']] = []
                        resps[resp['statusMessage']].append(unii1+":"+unii2)
                    else:
                        errors.append(resp['statusMessage'] + ":" + unii1 + ":" + unii2)
                        print unii1, unii2, resp
                        sys.exit()

    print len(errors)
    print errors
    for key in resps.keys():
        print key, len(resps[key]), resps[key]
