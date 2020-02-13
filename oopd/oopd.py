import csv
import sys
import urllib3
import json
from urllib.parse import quote

METAMAP = 'https://knowledge.ncats.io/ks/umls/metamap'
DISAPI = 'https://disease-knowledge.ncats.io/api'
DISEASE = DISAPI + '/search'

def parse_disease_map (codes, data):
    if len(data) > 0:
        for d in data:
            if 'I_CODE' in d:
                value = d['I_CODE']
                if isinstance (value, list):
                    for v in value:
                        codes[v] = None
                else:
                    codes[value] = None

def fetch_codes (http, url, codes):
    r = http.request('GET', url)
    data = json.loads(r.data.decode('utf-8'))['contents']
    parse_disease_map(codes, data)
    
def map_cui (cui, name):
    http = urllib3.PoolManager()
    codes = {}
    
    fetch_codes (http, DISEASE+'/UMLS:'+cui, codes)
    if len(codes) == 0:
        fetch_codes (http, DISEASE+'/'+quote(name, safe=''), codes)
        
    omim = []
    gard = []
    for k in codes.keys():
        if k.startswith('GARD:'):
            gard.append(k)
        elif k.startswith('OMIM:'):
            omim.append(k)
    if len(gard) == 0:
        # do expansion around omim
        for id in omim:
            fetch_codes (http, DISEASE+'/'+id, codes)

    codes = list(codes.keys())
    codes.sort()
    return codes

def fetch_node (path, node):
    if 'label' in node:
        path.append(node['label'])
        if 'children' in node:
            for n in node['children']:
                fetch_node(path, n)
    
    
def mondo_hierarchies (id):
    http = urllib3.PoolManager()
    r = http.request('GET', DISAPI+'/tree/'+id, fields={'field': 'label'})
    data = json.loads(r.data.decode('utf-8'))
    categories = []
    if 'children' in data:
        for n in data['children']:
            path = []
            fetch_node(path, n)
            # for now we only care about rare genetic disease
            if (len(path) > 0 and len(path) < 20
                and (path[0] == 'rare genetic disease'
                     or path[0] == 'inherited genetic disease')):
                categories.append(list(reversed(path)))
    return categories

    
def parse_metamap (data, *args):
    mapped = {}
    types = {}
    for st in args:
        types[st] = None
        
    for sent in data['utteranceList']:
        for token in sent['pcmlist']:
            if 'mappingList' in token:
                text = token['phrase']['phraseText']
                concepts = []
                seen = {}
                for map in token['mappingList']:
                    for ev in map['evList']:
                        cui = ev['conceptId']
                        name = ev['preferredName']
                        ## see this https://mmtx.nlm.nih.gov/MMTx/semanticTypes.shtml
                        for st in ev['semanticTypes']:
                            if st in types and cui not in seen:
                                if name != '0%':
                                    c = {
                                        'cui': cui,
                                        'name': name,
                                        'sty': st
                                    }
                                    
                                    if st == 'dsyn' or st == 'neop':
                                        maps = map_cui(cui, name)
                                        for id in maps:
                                            if (id.startswith('MONDO:')
                                                and id != 'MONDO:0000001'):
                                                cat = mondo_hierarchies(id)
                                                if len(cat) > 0:
                                                    c['categories'] = cat
                                        c['mapping'] = maps
                                    concepts.append(c)
                                    seen[cui] = None
                if len(concepts) > 0:
                    mapped[text] = concepts
    #print ('... %s => %s' % (text, concepts))
    return mapped
                        
def parse_oopd_file (file):
    http = urllib3.PoolManager()
    cache = {}
    with open (file) as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"')
        header = {}
        count = 0
        jstr = ''
        print ('[',end='')        
        for row in reader:
            if len(header) == 0:
                for i,n in enumerate (row):
                    header[n] = i
                if not 'Orphan Drug Status' in header:
                    raise Exception ('Not an OOPD file; please download from here https://www.accessdata.fda.gov/scripts/opdlisting/oopd/index.cfm!')
            else:
                designation = row[header['Designation']]
                #print (designation)
                resp = ''
                if designation in cache:
                    resp = cache[designation]
                else:
                    r = http.request(
                        'POST', METAMAP,
                        body=designation,
                        headers={'Content-Type': 'text/plain'}
                    )
                    resp = json.loads(r.data.decode('utf-8'))
                    cache[designation] = resp
                
                data = {'row': count+1}
                for k,v in header.items():
                    if v < len(row):
                        data[k] = row[v]
                data['DesignationMapped'] = parse_metamap (resp,
                                                           'dsyn',
                                                           'neop',
                                                           'fndg',
                                                           'gngm',
                                                           'comd',
                                                           'aapp',
                                                           'patf',
                                                           'ortf',
                                                           'fngs');
                indication = data['Approved Indication']
                if len(indication) > 0:
                    r = http.request(
                        'POST', METAMAP, body=indication,
                        headers={'Content-Type': 'text/plain'}
                    )
                    resp = json.loads(r.data.decode('utf-8'))
                    data['ApprovedIndicationMapped'] = parse_metamap(
                        resp, 'dsyn', 'neop')
            
                if len(jstr) > 0:
                    print (jstr, end=',')
                jstr = json.dumps(data, indent=4,separators=(',',': '))
                count += 1
#                if count > 10:
#                    break
        if len(jstr) > 0:
            print (jstr, end='')
        print (']')

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print ('usage: %s FILE' % (sys.argv[0]))
        sys.exit(1)
    parse_oopd_file (sys.argv[1])
