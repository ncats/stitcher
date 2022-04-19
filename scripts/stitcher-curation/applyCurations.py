#!/usr/bin/env python

import os
import sys
import requests
import json
import argparse

# check that the python version is correct (need 3)
if sys.version_info[0] < 3:
    raise "Must be using Python 3! Aborting."

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

def applyCuration(sline):
    obj = json.loads(sline[-1])
    url = site[:-1]+obj['_uri']
    badkeys = []
    for key in obj.keys():
        if key[0] == "_": # do not post parameters created by API
            badkeys.append(key)
    for key in badkeys:
        del obj[key]
    #print url, json.dumps(obj)

    try:
        req = requests.post(url, data=json.dumps(obj), headers={'Content-Type': 'application/json'})
        sys.stderr.write(req.text)
        sys.stderr.write("\n")
    except requests.exceptions.HTTPError as e:
        err = 'HTTP Error ERROR en el listado code => %s \n URL=> %s\n' % (e.code,url)
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
