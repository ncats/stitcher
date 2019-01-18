import os
import sys
import cookielib
import urllib
import urllib2
import json
import time

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

site = 'https://stitcher-dev.ncats.io/'
# site = 'http://localhost:8080/'

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

    filename = "dbCurations.txt"
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    
    fp = open(filename, "r")
    line = fp.readline()
    while line != "":
        applyCuration(line.split('\t'))
        line = fp.readline()
    fp.close()
