#!/usr/bin/env python

import os
import sys
import cookielib
import urllib
import urllib2
import json
import time
import argparse

# check that the python version is correct (need 2)
if sys.version_info[0] > 2:
    raise "Must be using Python 2! Aborting."

# check for arguments
args_p = argparse.ArgumentParser(description="Run Some Stitcher Tests")

args_p.add_argument('addr',
                    help="""a full Stitcher address OR
                            a shorthand: 'prod', 'dev', 'test' or 'local'""")

args_p.add_argument('--outfile',
                    default="".join(["dbCurations_",
                                     time.strftime("%Y-%m-%d", time.gmtime()),
                                     ".txt"]),
                    help="name of the output file")

site_arg = args_p.parse_args().addr
outfile = args_p.parse_args().outfile

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

def iterateCurations(fp):
    skip = 0
    top = 10
    max = 300000
    while skip < max:
        uri = site+'api/curations?top='+str(top)+'&skip='+str(skip)
        obj = requestJson(uri)
        if not obj.has_key('contents'):
            newobj = dict()
            newobj['contents'] = []
            newobj['contents'].append(obj)
            obj = newobj
            skip = max
        if obj is None:
            skip = skip
        elif len(obj['contents']) == 0:
            skip = max
        else:
            for entity in obj['contents']:
                items = []
                for entry in entity['_CURATION']:
                    obj2 = json.loads(entry)
                    items.append([obj2['_timestamp'], entry])
                items.sort()
                for item in items:
                    fp.write(str(entity['id'])+"\t"+str(entity['source'])+"\t"+str(entity['datasource'])+"\t"+str(item[-1])+"\n")
        sys.stderr.write(uri+"\n")
        sys.stderr.flush()
        skip = skip + top
    return

if __name__=="__main__":

    fp = open(outfile, "w")
    iterateCurations(fp)
    fp.close()
