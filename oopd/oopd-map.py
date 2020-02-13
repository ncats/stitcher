import csv
import sys
import urllib3
import json

DISAPI = 'https://disease-knowledge.ncats.io/api'
SEARCH = DISAPI + '/search'

def parse_disease_map (codes, genes, data):
    nodes = []
    if len(data) > 0:
        for d in data:
            orphan = False
            for l in d['labels']:
                if (l.startswith('S_FDAORPHANGARD')
                    or l.startswith('S_RANCHO-DISEASE')):
                    orphan = True
                    break
            if not orphan:
                if 'I_CODE' in d:
                    value = d['I_CODE']
                    if isinstance (value, list):
                        for v in value:
                            codes[v] = None
                    else:
                        codes[value] = None
                if 'I_GENE' in d:
                    for g in d['I_GENE']:
                        genes[g] = None
                    #print ('****** %d: %s' % (d['id'], d['I_GENE']))
                if 'neighbors' in d:
                    for n in d['neighbors']:
                        if n['reltype'] == 'N_Name' and len(n['value']) > 3:
                            nodes.append(n['node'])
    return nodes

def map_genes (genes):
    mapped = {}
    http = urllib3.PoolManager()
    for g in genes:
        if g.startswith('GENE:'):
            r = http.request('GET', SEARCH+'/'+g)
            data = json.loads(r.data.decode('utf-8'))['contents']
            for d in data:
                ogg = False
                for l in d['labels']:
                    if 'S_OGG' == l:
                        ogg = True
                        break
                if ogg:
                    p = d['payload'][0]
                    mapped[p['label']] = p['description']
                    break
    return mapped
                        
                    
def fetch_codes_and_genes (http, url, codes, genes):
    r = http.request('GET', url)
    data = json.loads(r.data.decode('utf-8'))['contents']
    nodes = parse_disease_map(codes, genes, data)
    if len(genes) == 0:
        #print ('%s => %s' % (url, nodes))
        for n in nodes:
            r = http.request('GET', DISAPI+'/node/'+str(n))
            data = []
            data.append(json.loads(r.data.decode('utf-8')))
            parse_disease_map(codes, genes, data)

def fetch_node (path, node):
    if 'label' in node:
        path.append(node['label'])
        if 'children' in node:
            for n in node['children']:
                fetch_node(path, n)
    
def genetic_info (id):
    http = urllib3.PoolManager()
    r = http.request('GET', DISAPI+'/tree/'+id, fields={'field': 'label'})
    data = json.loads(r.data.decode('utf-8'))
    info = []
    if 'children' in data:
        for n in data['children']:
            path = []
            fetch_node(path, n)
            # for now we only care about genetic disease
            if (len(path) > 0 
                and (path[0] == 'rare genetic disease'
                     or path[0] == 'inherited genetic disease')):
                info.append(path[0])
    return info
    

if len(sys.argv) == 1:
    print ('usage: %1 FILE' % (sys.argv[0]))
    sys.exit(1)

cache = {}
with open (sys.argv[1], encoding='mac_roman') as f:
    reader = csv.reader(f, delimiter='\t', quotechar='"')
    next(reader, None) # skip header
    http = urllib3.PoolManager()
    print ('Designation\tCount\tGenetic\tGenes')
    for row in reader:
        id = row[2].strip()
        if len(id) > 0:
            codes = {}
            genes = {}
            did = ''
            try:
                did = 'GARD:{:07d}'.format(int(id))
            except ValueError:
                if id.startswith('C'):
                    id = 'UMLS:'+id
                did = id
            out = ''
            if did not in cache:
                fetch_codes_and_genes (http, SEARCH+'/'+did, codes, genes)
                genetics = []
                for k in codes.keys():
                    if k.startswith('MONDO:'):
                        genetics = genetic_info(k)
                        break
                genes = map_genes(list(genes.keys()))
                out = '{}\t{}\t{}\t{}'.format(did, len(genes),
                                              len(genetics)>0,
                                              ','.join(list(genes.keys())))
                cache[did] = out
                print (out)
            
