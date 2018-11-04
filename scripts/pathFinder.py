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
    #print uri
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
            
def getEdges(nodes, edges = [], nodesrc = dict()):
    while len(nodes) > 0:
        newnodes = []
        for item in nodes:
            print nodes.index(item)
            uri = site + 'api/node/' + str(item)
            obj = requestJson(uri)
            nodesrc[item] = obj['payload'][-1]['id'].encode('utf-8') + "::".encode('utf-8') + obj['datasource']['name'].encode('utf-8')
            for entry in obj['payload']:
                if entry.has_key('PreferredName'):
                    nodesrc[item] = entry['id'].encode('utf-8') + ":" + entry['PreferredName'][0].encode('utf-8') + ":" + obj['datasource']['name'].encode('utf-8')
            if obj.has_key('neighbors'):
                for entry in obj['neighbors']:
                    if entry.has_key('value') and entry['reltype'] != "H_LyChI_L3":
                        newEdge = [item, entry['id'], entry['reltype'], entry['value']]
                        if newEdge not in edges:
                            edges.append(newEdge)
                            if entry['id'] not in nodes and entry['id'] not in nodesrc.keys() and len(nodesrc.keys()) < 100 and len(newnodes) < 100:
                                newnodes.append(entry['id'])
                                #print str(item)+":"+str(entry['id']) + ":" + entry['reltype'] + ":" + entry['value']
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

def extendPaths(paths, edges, visited = []):
    npaths = []
    for path in paths:
        for edge in edges:
            if edge[0] == path[-1] and edge[1] not in path and edge[1] not in visited:
                npath = list(path)
                npath.append(edge[1])
                npaths.append(npath)
                visited.append(edge[1])
    return npaths, visited

def getEdgesSimple(nodes, edges = [], nodesrc = dict()):
    for item in nodes:
        if item not in nodesrc.keys():
            uri = site + 'api/node/' + str(item)
            obj = requestJson(uri)
            nodesrc[item] = obj['payload'][-1]['id'].encode('utf-8') + "::".encode('utf-8') + obj['datasource']['name'].encode('utf-8')
            for entry in obj['payload']:
                if entry.has_key('PreferredName'):
                    nodesrc[item] = str(entry['id']) + ":" + str(entry['PreferredName'][0]) + ":" + str(obj['datasource']['name'])
            if obj.has_key('neighbors'):
                for entry in obj['neighbors']:
                    if entry.has_key('value') and entry['reltype'] != "H_LyChI_L3":
                        newEdge = [item, entry['id'], entry['reltype'], entry['value']]
                        if newEdge not in edges:
                            edges.append(newEdge)
    return edges, nodesrc

def extendPathsSimple(paths, edges, nodesrc, visited = [], extend = True):
    npaths = []
    newnodes = []
    for path in paths:
        for edge in edges:
            if edge[0] == path[-1] and edge[1] not in path and edge[1] not in visited:
                npath = list(path)
                npath.append(edge[1])
                npaths.append(npath)
                visited.append(edge[1])
                newnodes.append(edge[1])
    if len(newnodes) > 30:
        newnodes = newnodes[:30]
    if extend:
        edges, nodesrc = getEdgesSimple(newnodes, edges, nodesrc)
    return npaths, edges, nodesrc, visited

def getSource(member):
    if member.has_key('name'):
        return member['id'] + ":" + member['name'] + ":" + member['source']
    return member['id'] + "::" + member['source']

def probeUniiClash(stitch1, node1, stitch2, node2):
    nodesrc = dict()
    stitch = []
    uri = site + 'api/stitches/latest/'+stitch1
    obj = requestJson(uri)
    if node1 is None:
        node1 =  obj['sgroup']['parent']
    for member in obj['sgroup']['members']:
        if member['id'] == node1 or member['node'] == node1:
            stitch.append(member['node'])
            nodesrc[member['node']] = getSource(member)

    ostitch = []
    uri = site + 'api/stitches/latest/'+stitch2
    obj = requestJson(uri)
    if node2 is None:
        node2 = obj['sgroup']['parent']
    for member in obj['sgroup']['members']:
        if member['id'] == node2 or member['node'] == node2:
            ostitch.append(member['node'])
            nodesrc[member['node']] = getSource(member)

    edges = []
    nodes = []
    for item in stitch:
        uri = site + 'api/node/' + str(item)
        objn = requestJson(uri)
        nodes.append(objn['parent'])
        edges.append([item, objn['parent'], 'parent', 'parent'])
    for item in ostitch:
        uri = site + 'api/node/' + str(item)
        objn = requestJson(uri)
        nodes.append(objn['parent'])
        edges.append([objn['parent'], item, 'rparent', 'rparent'])

    # if asked to map out path within a single stitchnode only use nodes from that stitchnode
    if stitch1 == stitch2:
        for member in obj['sgroup']['members']:
            uri = site + 'api/node/' + str(member['node'])
            objn = requestJson(uri)
            if objn['parent'] not in nodes:
                nodes.append(objn['parent'])
        
    edges, nodesrc = getEdgesSimple(nodes, edges, nodesrc)

    paths = []
    for item in stitch:
        paths.append([item])

    #print stitch, ostitch, len(nodesrc), len(edges)
    visited = stitch
    match = []
    round = 0
    while len(match) == 0:
        for p in paths:
            if p[-1] in ostitch:
                match.append(list(p))
        if len(match) == 0:
            round = round + 1
            paths, edges, nodesrc, visited = extendPathsSimple(paths, edges, nodesrc, visited, stitch1 != stitch2)
            #print round, len(paths)
            if len(paths) == 0 or round > 10:
                match = ["impossible"]

    print stitch1, node1, stitch2, node2
    if match == ["impossible"]:
        print "Impossible!!!"
    else:
        for m in match:
            print
            print "Match:", m
            for i in range(0, len(m)-1):
                print "Node:", m[i], nodesrc[m[i]]
                for e in edges:
                    if e[0] == m[i] and e[1] == m[i+1]:
                        print "Edge", i+1, e
            print "Node:", m[-1], nodesrc[m[-1]]
    print "--------"
    print
    sys.stdout.flush()
    
if __name__=="__main__":

    #probeUniiClash('0GEI24LG0J', '0GEI24LG0J', '0GEI24LG0J', '724L30Y2QR') # NPC has swapped UNIIs for chenodiol and ursodiol
    #probeUniiClash('059QF0KO0R', '059QF0KO0R', '059QF0KO0R', '1GA689N629')
    #probeUniiClash('059QF0KO0R', None, '059QF0KO0R', '451W47IQ8X')
    
    #Good test cases
    #probeUniiClash('793513', None, '793526', None)
    #probeUniiClash('359HUE8FJC', None, '359HUE8FJC', 'famciclovir') # Rancho provides two UNIIs that are not synonymous
    #probeUniiClash('13S1S8SF37', None, '13S1S8SF37', '2GFP9BJD79')
    #probeUniiClash('0GEI24LG0J', '0GEI24LG0J', '0GEI24LG0J', '724L30Y2QR')
    #sys.exit()

    if len(sys.argv) < 2:
        print "USAGE: python pathFinder.py [stitcherRegression outputfile]"
        sys.exit()
    
    lines = open(sys.argv[1], "r").readlines()
    step = 1
    for line in lines:
        if step == 1:
            header = line
            step = 0
        elif line.strip() == "":
            step = 1
        else:
            sline = line[0:-1].split("\t")
            if sline[0] == 'nmeClashes' or sline[0] == 'nmeClashes2' or sline[0] == 'PMEClashes' or sline[0] == 'activemoietyClashes':
                aunii = sline[1]
                anchor = sline[3]
                for i in range(5, len(sline), 2):
                    probeUniiClash(aunii, aunii, aunii, sline[i])
            else:
                print sline
                sys.exit()
