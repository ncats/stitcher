import os, sys, json, requests, argparse

LUT = {}
API = {}
TOKEN = ''

def set_id(s, d):
    if 'Id' in d:
        s['Id'] = d['Id']

def set_gene_id(s, d):
    if 'Id' in d:
        s['Id'] = d['Id']
        s['gene_sfdc_id'] = d['Gene_Gene_Symbol__c']

def set_phenotype_id(s, d):
    if 'Id' in d:
        s['Id'] = d['Id']        
        s['phenotype_sfdc_id'] = d['Feature__c']
    
def _smash(array, r, type, fn = set_id):
    data = []
    for x in r:
        if x['attributes']['type'] == type:
            data.append(x)
    for (i,s) in enumerate(array):
        fn(s, data[i])
    
def smash(d, r):
    _smash (d['synonyms'], r, 'Disease_Synonym__c')
    _smash (d['external_identifiers'], r, 'External_Identifier_Disease__c')
    _smash (d['inheritance'], r, 'Inheritance__c')
    _smash (d['age_at_onset'], r, 'Age_At_Onset__c')
    _smash (d['age_at_death'], r, 'Age_At_Death__c')
    _smash (d['epidemiology'], r, 'Epidemiology__c')
    _smash (d['genes'], r, 'GARD_Disease_Gene__c', set_gene_id)
    _smash (d['phenotypes'], r, 'GARD_Disease_Feature__c', set_phenotype_id)
    _smash (d['evidence'], r, 'Evidence__c')


def get_auth_token(url, params):
    r = requests.post(url, data=params)
    if r.status_code != 200:
        print('%s returns status code %d!' % (
            r.url, r.status_code), file=sys.stderr)
        return None
    #print(json.dumps(r.json(), indent=2))
    return r.json()['access_token']

def parse_config(file):
    with open(file) as f:
        config = json.load(f)
        if 'api' not in config or 'params' not in config:
            print('** %s: not a valid config file! **' % file, file=sys.stderr)
            sys.exit(1)
        return config

def load_objects(url, data, path, prefix):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer %s' % TOKEN
    }
    total = 0
    batch = 1
    for d in data:
        r = requests.patch(url, headers=headers, json=d)
        with open(path+'/%s_%05d.json' % (prefix, batch), 'w') as f:
            resp = {
                'status': r.status_code,
                'input': d,
                'response': r.json()
            }
            print (json.dumps(resp, indent=2), file=f)
        batch = batch + 1
        if 200 == r.status_code:
            total = total + len(d['records'])
        print('%d - %d' % (r.status_code, total))
    print ('%d record(s) loaded for %s' % (total, url))
    return total

def load_genes(file, out):
    with open(file, 'r') as f:
        data = json.load(f)
        load_objects(API['gene'], data, out, 'gene')

def load_phenotypes(file, out):
    with open(file, 'r') as f:
        data = json.load(f)
        load_objects(API['phenotype'], data, out, 'phenotype')

def load_disease(d, path='.'):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer %s' % TOKEN
    }
    term = d['term']
    term['Id'] = LUT[term['curie']]
    r = requests.post(API['disease'], headers=headers, json=d)
    
    base = path+'/'+term['curie'].replace(':', '_')
    sf = r.json()
    with open(base+'_sf.json', 'w') as f:
        print (json.dumps(sf, indent=2), file=f)
    with open(base+'_%d.json' % r.status_code, 'w') as f:
        smash(d, sf)
        print (json.dumps(d, indent=2), file=f)
        
    return (r.status_code, term['curie'], term['Id'])
    
def load_diseases(file, out):
    with open(file, 'r') as f:
        data = json.load(f)
        total = 0
        count = 0
        for d in data:
            r = load_disease(d, out)
            print('%d -- %d/%s %s %s' % (r[0], total, count, r[1], r[2]))
            if 200 == r[0]:
                total = total + 1
            count = count + 1
        print('-- %d total record(s) loaded!' % total)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True,
                        help='Required configuration in json format')
    parser.add_argument('-t', '--type', required=True,
                        choices=['gene', 'phenotype','disease'],
                        help='Input file is of specific type')
    parser.add_argument('-m', '--mapping', default='gard_diseases_sf.csv',
                        help='Salesforce disease mapping file (default: gard_diseases_sf.csv)')
    parser.add_argument('-o', '--output', help='Output path (default .)')    
    parser.add_argument('FILE', help='Input JSON file')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    print('-- type: %s' % args.type)
    print('-- config: %s' % args.config)
    print('-- output: %s' % args.output)
    print('-- FILE: %s' % args.FILE)
    
    config = parse_config(args.config)
    API = config['api']
    TOKEN = get_auth_token(API['token'], config['params'])
    print('-- TOKEN: %s...' % TOKEN[0:32])

    try:
        os.makedirs(args.output)
    except FileExistsError:
        pass
    
    if args.type == 'disease':
        print('-- mapping: %s' % args.mapping)        
        with open(args.mapping) as f:
            f.readline() # skip header
            for line in f.readlines():
                gard, sfid = line.strip().split(',')
                #print('%s -- %s' % (gard, sfid))
                LUT[gard] = sfid
                #print('%d: %s %s!' % (len(LUT), gard, sfid))
            print('-- %d mappings loaded!' % (len(LUT)))
        load_diseases(args.FILE, args.output)
        
    elif args.type == 'gene':
        load_genes(args.FILE, args.output)

    elif args.type == 'phenotype':
        load_phenotypes(args.FILE, args.output)
