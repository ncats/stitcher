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
site = 'https://stitcher.ncats.io/'
site = 'http://localhost:8080/'

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

def iterateStitches(funcs):
    dicts = []
    for item in funcs:
        dicts.append(dict())
    skip = 0
    top = 10
    max = 300000
    while skip < max:
        uri = site+'api/stitches/v1?top='+str(top)+'&skip='+str(skip)
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
            for stitch in obj['contents']:
                for i in range(len(funcs)):
                    funcs[i](dicts[i], stitch)
        sys.stderr.write(uri+"\n")
        sys.stderr.flush()
        skip = skip + top
    return dicts

def getName(obj):
    name = ""
    for member in obj['sgroup']['members']:
        if name == "":
            if member.has_key('name'):
                name = member['name']
            elif member['stitches'].has_key('N_Name'):
                if len(member['stitches']['N_Name'][0]) == 1:
                    name = str(member['stitches']['N_Name'])
                else:
                    name = str(member['stitches']['N_Name'][0])
    if name == "":
        if obj['sgroup']['properties'].has_key('Synonyms'):
            if not isinstance(obj['sgroup']['properties']['Synonyms'], (list, tuple)):
                name = obj['sgroup']['properties']['Synonyms']['value']
            else:
                name = obj['sgroup']['properties']['Synonyms'][0]['value']
    if name == "":
        if obj['sgroup']['properties'].has_key('unii'):
            name = obj['sgroup']['properties']['unii'][0]['value']
    return name
            
def countGenerics(drugs, stitch):
    status = 'Other'
    if stitch.has_key('highestPhase'):
        status = stitch['highestPhase']
        for event in stitch['events']:
            if event['id'] == status:
                status = event['kind']
    if stitch.has_key('events'):
        for event in stitch['events']:
            if event['kind'] == 'USApprovalOTC':
                status = 'generic'
            if event['kind'] == 'USApprovalRx':
                if event.has_key('approvalAppId') and event['approvalAppId'][0:4] == 'ANDA':
                    status = 'generic'
                if event.has_key('marketingStatus') and event['marketingStatus'][0:4] == 'ANDA':
                    status = 'generic'
    parent = stitch['sgroup']['parent']
    rank = stitch['rank']
    unii = ''
    name = ''
    for node in stitch['sgroup']['members']:
        if node['node'] == parent:
            if 'id' in node:
                unii = node['id']
            elif 'name' in node:
                unii = node['name']
            else:
                sys.stderr.write("failed parent node: "+str(parent)+"\n")
                sys.stderr.flush()
            name = getName(stitch)
    if status != 'Other':
        drugs[unii] = [name, status, stitch['id'], rank]
    return drugs

if __name__=="__main__":
    tests = [countGenerics]
    testHeaders = dict()
    testHeaders['countGenerics'] = 'drugs\tUNII\tPN\tStitch Node\tStitch Rank\tClash UNII 1\tClash PN 1\tClash UNII 2\tClash PN 2\tetc.'

    # initialize UNII preferred terms
    uniis = dict()
    fp = open('../../temp/UNIIs-2019-06-18/UNII NAMES 7Mar2019.txt', 'r')
    line = fp.readline()
    line = fp.readline()
    while line != "":
        sline = line[0:-2].split("\t")
        uniis[sline[2]] = sline[3]
        line = fp.readline()
    fp.close()
    
    outputs = iterateStitches(tests)
    # write out results
    for i in range(len(tests)):
        test = tests[i].__name__
        print testHeaders[test]
        output = outputs[i]
        keys = output.keys()
        keys.sort()
        for key in keys:
            keyname = ""
            if uniis.has_key(key):
                keyname = uniis[key]
            outline = test + "\t" + key.encode('ascii', 'ignore') + "\t" + keyname.encode('ascii', 'ignore')
            for item in output[key]:
                outline = outline + "\t" + unicode(item).encode('ascii', 'ignore')
                if uniis.has_key(item):
                    outline = outline + "\t" + uniis[item]
            print outline  
    sys.exit()
 
    
