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

def applyCuration(sline):
    obj = json.loads(sline[-1])
    url = site[:-1]+obj['_uri']
    for key in obj.keys():
        if key[0] == "_": # do not post parameters created by API
            del obj[key]
    #print url, json.dumps(obj)

    req = urllib2.Request(url, json.dumps(obj), {'Content-Type': 'application/json'})
    try:
        html = urllib2.urlopen(req)
        sys.stderr.write(html.read())
        sys.stderr.write("\n")
    except urllib2.HTTPError, e:
        err = 'HTTP Error ERROR en el listado code => %s \n URL=> %s\n' % (e.code,url)
        sys.stderr.write(err)
        sys.exit()
    except urllib2.URLError, e:
        err = 'URL Error ERROR en el listado reason => %s \n URL=> %s\n' % (e.reason,url)
        sys.stderr.write(err)
        sys.exit()
    return

if __name__=="__main__":

    fp = open(filename, "r")

    line = fp.readline()

    while line != "":
        applyCuration(line.split('\t'))
        line = fp.readline()
    fp.close()
