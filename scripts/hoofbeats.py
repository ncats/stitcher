import os, sys, json, requests, argparse, traceback

LUT = {}
API = {}
GENES = {}
PHENOTYPES = {}
TAGS = {}
DISEASES = {}
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

def set_drug_id(s, d):
    if 'Id' in d:
        s['Id'] = d['Id']
        s['drug_sfdc_id'] = d['Drug__c']
    
def _smash(array, r, type, fn = set_id):
    data = []
    for x in r:
        if x['attributes']['type'] == type:
            data.append(x)
    for (i,s) in enumerate(array):
        fn(s, data[i])

def smash(d, r):
    _smash (d['disease_categories'], r, 'GARD_Disease_Category__c')
    _smash (d['synonyms'], r, 'Disease_Synonym__c')
    _smash (d['external_identifiers'], r, 'External_Identifier_Disease__c')
    _smash (d['inheritance'], r, 'Inheritance__c')
    _smash (d['age_at_onset'], r, 'Age_At_Onset__c')
    _smash (d['age_at_death'], r, 'Age_At_Death__c')
    _smash (d['diagnosis'], r, 'Diagnosis__c')
    _smash (d['epidemiology'], r, 'Epidemiology__c')
    _smash (d['genes'], r, 'GARD_Disease_Gene__c', set_gene_id)
    _smash (d['phenotypes'], r, 'GARD_Disease_Feature__c', set_phenotype_id)
    _smash (d['drugs'], r, 'GARD_Disease_Drug__c', set_drug_id)
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

def load_drugs(file, out):
    with open(file, 'r') as f:
        data = json.load(f)
        load_objects(API['drug'], data, out, 'drug')

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
        if r.status_code == 200:
            smash(d, sf)
        print (json.dumps(d, indent=2), file=f)
        
    return (r.status_code, term['curie'], term['Id'])

def reload_disease(file):
    with open(file, 'r') as f:
        data = json.load(f)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % TOKEN
        }
        r = requests.post(API['disease'], headers=headers, json=data)
        print ('%s -- %d' % (file, r.status_code))
        print (json.dumps(r.json(), indent=2))

    
def load_diseases(file, out, **kwargs):
    with open(file, 'r') as f:
        curies = {}
        if 'include' in kwargs and kwargs['include'] != None:
            for c in kwargs['include']:
                curies[c] = None
        start = 0
        if 'skip' in kwargs and kwargs['skip'] != None:
            start = kwargs['skip']
        data = json.load(f)
        total = 0
        count = 0
        if isinstance(data, list):
            for d in data:
                if ((len(curies) == 0 or d['term']['curie'] in curies)
                    and (start == 0 or count > start)):
                    r = load_disease(d, out)
                    print('%d -- %d/%s %s %s' % (r[0], total, count, r[1], r[2]))
                    if 200 == r[0]:
                        total = total + 1
                count = count + 1
            print('-- %d total record(s) loaded!' % total)
        else:
            load_disease(data, out)

def patch_objects1(target, source, field, *fields):
    if field in source:
        sfids = {}
        for s in source[field]:
            sfids[s['curie']] = s
        for t in target[field]:
            if t['curie'] in sfids:
                d = sfids[t['curie']]
                t['Id'] = d['Id']
                for f in fields:
                    t[f] = d[f]
        
def patch_objects2(target, source, field):
    if field in source:
        sfids = {}        
        for s in source[field]:
            if s['curie'] in sfids:
                sfids[s['curie']][s['label']] = s['Id']
            else:
                sfids[s['curie']] = {s['label']: s['Id']}
        patches = []
        processed = {}
        for t in target[field]:
            if t['curie'] in sfids and t['label'] in sfids[t['curie']]:
                id = sfids[t['curie']][t['label']]
                if id not in processed:
                    t['Id'] = id
                    processed[id] = None
                    patches.append(t)
            else:
                patches.append(t)
            
        if len(patches) > 0:
            target[field] = patches
            
def patch_evidence(target, source):
    sfids = {}
    for e in source['evidence']:
        sfids[e['url']] = e['Id']
    for e in target['evidence']:
        if e['url'] in sfids:
            e['Id'] = sfids[e['url']]

def patch_objects(target, source, field, minlen=0):
    if field in source and field in target:
        patches = []
        processed = {}
        for t in target[field]:
            best_s = {}
            ovbest = {}
        
            t['Id'] = ''
            for s in source[field]:
                ov = {k: t[k] for k in t if k in s and s[k] == t[k]}
                if len(ov) > len(ovbest):
                    best_s = s
                    ovbest = ov
            if len(ovbest) > minlen:
                t['Id'] = best_s['Id']
                if t['Id'] not in processed:
                    patches.append(t)
                    processed[t['Id']] = None
            else:
                patches.append(t)
            
        if len(patches) > 0:
            target[field] = patches
            
    elif field in source:
        # deleted field
        print("** target object doesn't have field '%s'; "
              "deletion is currently not supported!" % field, file=sys.stderr)
    
def patch_disease(disease, in_dir, out_dir):
    file = in_dir+'/%s_200.json' % disease['term']['curie'].replace(':','_')
    r = ()
    if os.access(file, os.R_OK):
        with open(file, 'r') as f:
            d = json.load(f)
            #print(json.dumps(d, indent=2))
            disease['term']['Id'] = d['term']['Id']
            patch_objects2 (disease, d, 'disease_categories')
            patch_objects2 (disease, d, 'synonyms')
            patch_objects (disease, d, 'external_identifiers', 2)
            patch_objects2 (disease, d, 'inheritance')
            patch_objects2 (disease, d, 'age_at_onset')
            patch_objects2 (disease, d, 'age_at_death')
            patch_objects (disease, d, 'diagnosis', 1)
            patch_objects (disease, d, 'epidemiology')
            patch_objects1 (disease, d, 'genes', 'gene_sfdc_id')
            patch_objects1 (disease, d, 'phenotypes', 'phenotype_sfdc_id')
            patch_objects1 (disease, d, 'drugs', 'drug_sfdc_id')
            patch_evidence (disease, d)
            #print(json.dumps(disease, indent=2))
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer %s' % TOKEN
            }
            r = requests.post(API['disease'], headers=headers, json=disease)
            file = (out_dir+'/'+disease['term']['curie'].replace(':','_')+'_%d'
                    % r.status_code)
            sf = r.json()
            with open(file+'_sf.json', 'w') as f:
                print (json.dumps(sf, indent=2), file=f)
            print('%d...patching %s' % (r.status_code, disease['term']['curie']))
            with open(file+'.json', 'w') as f:
                if 200 == r.status_code:
                    smash(disease, sf)
                print (json.dumps(disease, indent=2), file=f)
    else:
        r = load_disease(disease, out_dir)
    return r

def patch_diseases(file, in_dir, out_dir, include):
    with open(file, 'r') as f:
        curies = {}
        if include != None:
            for c in include:
                curies[c] = None
        data = json.load(f)
        if isinstance(data, list):
            for d in data:
                if len(curies) == 0 or d['term']['curie'] in curies:
                    patch_disease(d, in_dir, out_dir)
        else:
            patch_disease(data, in_dir, out_dir)

class DiseaseUpdate:
    def __init__(self, d):
        self.term = d['term']['curie']
        data = self.query(
            "SELECT Id FROM GARD_Disease__c where DiseaseID__c = '%s'"
            % self.term)
        #print(json.dumps(data,indent=2))
        self.d = None # latest d
        self.Id = None
        if data != None:
            d['term']['Id'] = self.Id = data['Id']
            self.d = d
        # salesforce version of d                   
        self.sd = self.fetch_sf_disease(self.term)
        # changes as compared with the version in salesforce
        self.changes = []
                         
    @staticmethod
    def query(q):
        headers = {
            'Authorization': 'Bearer %s' % TOKEN
        }
        #print('Query: %s...' % q)    
        r = requests.get(API['query'], params={'q': q},  headers = headers)
        if 200 == r.status_code:
            data = r.json()
            size = data['totalSize']
            if size == 1:
                return data['records'][0]
            elif size > 1:
                return data['records']
        else:
            print('error: query returns status %d\n%s' % (
                r.status_code, q), file=stderr)
        return None

    @staticmethod
    def fetch_sf_disease(term):
        headers = {
            'Authorization': 'Bearer %s' % TOKEN
        }
        r = requests.get(API['disease'],
                         params={ 'term': term}, headers=headers)
        if 200 == r.status_code:
            return r.json()
        else:
            print('error: %s returns status %d' % (
                API['disease'], r.status_code), file=sys.stderr)
        return None
        
    def instrument(self):
        if self.Id == None:
            return
        self.categories()
        self.synonyms()
        self.external_identifiers()
        self.inheritance()
        self.age_at_onset()
        self.age_at_death()
        self.diagnosis()
        self.epidemiology()
        self.genes()
        self.phenotypes()
        self.drugs()
        self.evidence()
        self.related_diseases()
        self.tags()
        #print(json.dumps(self.d, indent=2))

    def commit(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % TOKEN
        }
        return requests.post(API['disease'], headers=headers, json=self.d)

    def categories(self):
        data = self.query("""
SELECT Id,Disease_Category_Curie__c,Disease_Category__c 
FROM GARD_Disease_Category__c where GARD_Disease__c = '%s'
""" % self.Id)
        if data != None:
            if isinstance(data, list):
                categories = {x['Disease_Category_Curie__c']: {
                    'Id': x['Id'],
                    'category_sfdc_id': x['Disease_Category__c']
                } for x in data}
            else:
                categories = {
                    data['Disease_Category_Curie__c']: {
                        'Id': data['Id'],
                        'category_sfdc_id': data['Disease_Category__c']
                    }
                }
            for x in self.d['disease_categories']:
                c = categories[x['curie']]
                if c != None:
                    x['Id'] = c['Id']
                    x['category_sfdc_id'] = c['category_sfdc_id']
                else:
                    x['Id'] = ''
                    x['category_sfdc_id'] = ''
                    self.changes.append(x)

    def synonyms(self):
        self.set_id('synonyms', lambda x: self.key(x['curie'], x['label']),
                    'Disease_Synonym__c', lambda x: self.key(
                        x['Disease_Synonym_Source__c'],
                        x['Disease_Synonym__c']))

    def external_identifiers(self):
        self.set_id('external_identifiers', lambda x: self.key(
            x['source'],x['curie']), 'External_Identifier_Disease__c',
                    lambda x: self.key(x['Source__c'], x['Display_Value__c']))
        
    def inheritance(self):
        keyf = lambda x: self.key(x['curie'], x['label'])
        keyt = lambda x: self.key(
            x['Inheritance_Attribution_Source__c'],
            x['Inheritance_Attribution__c'])
        self.set_id('inheritance', keyf, 'Inheritance__c', keyt)

    def age_at_onset(self):
        self.set_id('age_at_onset', lambda x: self.key(x['curie'], x['label']),
                    'Age_At_Onset__c', lambda x: self.key(
                        x['AgeAtOnset_AttributionSource__c'],
                        x['Age_At_Onset__c']))

    def age_at_death(self):
        self.set_id('age_at_death', lambda x: self.key(x['curie'], x['label']),
                    'Age_At_Death__c', lambda x: self.key(
                        x['AgeAtDeath_AttributionSource__c'],
                        x['Age_At_Death__c']))

    def diagnosis(self):
        self.set_id('diagnosis', lambda x: self.key(x['type'], x['curie']),
                    'Diagnosis__c', lambda x: self.key(
                        x['Type__c'], x['Curie__c']))

    def epidemiology(self):
        def keyf(x):
            try:
                return self.key(
                    x['geographic'], x['type'],
                    x['qualification'], x['valmoy'])
            except KeyError:
                print('KeyError: in %s' % x, file=sys.stderr)
                traceback.print_exc()
                
        def keyt(x):
            try:
                return self.key(
                    x['Location__c'], x['Type__c'],
                    x['Qualification__c'], x['Value__c'])
            except KeyError:
                print('KeyError: in %s' % x, file=sys.stderr)
                traceback.print_exc()
                
        self.set_id('epidemiology', keyf, 'Epidemiology__c', keyt)

    def genes(self):
        def get_sfdc_id(x):
            # we also need to set gene_sfdc_id
            sfdc = None
            if x['gene_symbol'] in GENES:
                sfdc = GENES[x['gene_symbol']]

            if sfdc == None:
                data = self.query("SELECT Id FROM Gene__c where Name = '%s'"
                                  % x['gene_symbol'])
                if data != None:
                    if isinstance(data, list) and len(data) > 0:
                        print('%s: has %d genes in salesforce!' %(
                            x['gene_symbol'], len(data)), file=sys.stderr)
                        data = data[0]
                    GENES[x['gene_symbol']] = sfdc = data['Id']
                else:
                    print('%s: gene not found in salesforce!'
                          % x['gene_symbol'], file=sys.stderr)
            x['gene_sfdc_id'] = sfdc
            
        self.set_id('genes', lambda x: x['gene_symbol'], 'GARD_Disease_Gene__c',
                    lambda x: x['GeneSymbol__c'], get_sfdc_id)

    def phenotypes(self):
        def get_sfdc_id(x):
            sfdc = None
            if x['curie'] in PHENOTYPES:
                sfdc = PHENOTYPES[x['curie']]

            if sfdc == None:
                data = self.query("""
SELECT Id FROM Feature__c where External_ID__c = '%s'
""" % x['curie'])
                if data != None:
                    if isinstance(data, list) and len(data) > 0:
                        print('%s: has %d features in salesforce!' %(
                            x['label'], len(data)), file=sys.stderr)
                        data = data[0]
                    PHENOTYPES[x['curie']] = sfdc = data['Id']
                else:
                    print('%s: phenotype not found in salesforce!'
                          % x['curie'], file=sys.stderr)
            if sfdc != None:
                x['phenotype_sfdc_id'] = sfdc

        #print(json.dumps(self.d['phenotypes'], indent=2), file=sys.stderr)
        self.set_id('phenotypes', lambda x: x['label'],
                    'GARD_Disease_Feature__c', lambda x: x['HPO_Name__c'],
                    get_sfdc_id)
        #print(json.dumps(self.d['phenotypes'], indent=2), file=sys.stderr)

    def drugs(self):
        self.set_id('drugs', lambda x: x['curie'], 'GARD_Disease_Drug__c',
                    lambda x: x['Drug_Curie__c'])

    def evidence(self):
        self.set_id('evidence', lambda x: x['label'], 'Evidence__c',
                    lambda x: x['Evidence_Label__c'])

    def related_diseases(self):
        def get_sfdc_id(x):
            sfdc = None
            if x['curie'] in DISEASES:
                sfdc = DISEASES[x['curie']]
                
            if sfdc == None:
                data = self.query(
                    "select Id from GARD_Disease__c where DiseaseID__c = '%s'"
                    % x['curie'])
                if data != None:
                    if isinstance(data, list) and len(data) > 0:
                        print('%s has %d diseases in salesforce!' % (
                            x['curie'], len(data)), file=sys.stderr)
                        data = data[0]
                    DISEASES[x['curie']] = sfdc = data['Id']
                else:
                    print('%s: disease not found in salesforce!'
                          % x['curie'], file=sys.stderr)
            x['disease_sfdc_id'] = sfdc
            
        self.set_id('related_diseases', lambda x: x['curie'],
                    'Related_Disease__c', lambda x: x['Related_Disease_ID__c'],
                    get_sfdc_id)

    def tags(self):
        def get_sfdc_id(x):
            sfdc = None
            if x['curie'] in TAGS:
                sfdc = TAGS[x['curie']]
                
            if sfdc == None:
                data = self.query(
                    "select Id from Tag__c where Unique_Identifier__c='%s'"
                    % x['curie'])
                if data != None:
                    TAGS[x['curie']] = sfdc = data['Id']
                else:
                    print('%s: tag not found in salesforce 🙄!' % x['curie'],
                          file=sys.stderr)
            x['tag_sfdc_id'] = sfdc
            
        def keyf(x):
            try:
                return self.key(x['category'], x['label'])
            except KeyError:
                print('KeyError in %s' % x, file=sys.stderr)
                traceback.print_exc()
                
        def keyt(x):
            try:
                return self.key(x['Tag_Category__c'], x['Tag_Name__c'])
            except KeyError:
                print('KeyError in %s' % x, file=sys.stderr)
                trackeback.print_exc()
                
        self.set_id('tags', keyf, 'GARD_Disease_Tag__c', keyt, get_sfdc_id)

    def set_id(self, f, keyf, t, keyt, postfn = None):
        try:
            lut = {
                keyt(x): x['Id']
                for x in list(filter(
                        lambda x: x['attributes']['type']==t, self.sd))}

            if lut:
                seen = set()
                values = list()
                dups = 0
                for x in self.d[f]:
                    key = keyf(x)
                    if key in lut:
                        id = lut[key]
                        if id not in seen:
                            x['Id'] = id
                            values.append(x)
                            seen.add(id)
                        else:
                            print('%s: warning: duplicate key found: %s (%s)'
                                  % (self.term, key, id), file=sys.stderr)
                            dups = dups + 1
                    else:
                        print('%s: key "%s" not found for "%s"; treating as new'
                              % (self.term, key, f), file=sys.stderr)
                        self.changes.append(key)
                        values.append(x)
                        
                    if postfn != None:
                        postfn(x)

                    #print(json.dumps(x, indent=2), file=sys.stderr)

                if dups > 0:
                    # update the field if there are dups
                    self.d[f] = values
                    
            elif self.d[f]:
                print('%s: no data for "%s" in salesforce; treating as new' % (
                    self.term, f), file=sys.stderr)
            
        except KeyError:
            print('** warning: %s: KeyError for type %s in %s!' % (
                self.term, t, f), file=sys.stderr)

    @staticmethod
    def key(*kargs):
        return '/'.join(kargs).lower()


def update_disease(d, f, e):
    du = DiseaseUpdate(d)
    du.instrument()
    r = du.commit()
    print('### %s...%s' % (
        d['term']['curie'], r.status_code), file=sys.stderr)
    if 200 == r.status_code:
        print(json.dumps(r.json(), indent=2), file=f)
    else:
        err = {
            'term': du.term,
            'status': r.status_code,
            'mesg': r.json()
        }
        print(json.dumps(err, indent=2), file=e)
    
def update_diseases(file, out, err, include = None):
    with open(file, 'r') as f, open(out, 'w') as o, open(err, 'w') as e:
        curies = {}
        if include != None:
            for c in include:
                curies[c] = None
        data = json.load(f)
        if isinstance(data, list):
            for d in data:
                if len(curies) == 0 or d['term']['curie'] in curies:
                    update_disease(d, o, e)
        else:
            update_disease(data, o, e)
            
def fetch_term(term):
#    print('fetching terms...%s' % term)
    headers = {
        'Authorization': 'Bearer %s' % TOKEN
    }
    
    r = requests.get(API['disease'], params={ 'term': term }, headers=headers)
    if 200 == r.status_code:
        print (json.dumps(r.json(), indent=2))
    else:
        print ('%s: returns status code %d for term %s'
               % (API['disease'], r.status_code, t), file=sys.stderr)

def browse():
    headers = {
        'Authorization': 'Bearer %s' % TOKEN,
        'X-PrettyPrint': '1'
    }
    q = API['query']+"?q=SELECT Id,Name,DiseaseID__c FROM GARD_Disease__c WHERE DiseaseID__c >= 'GARD:0000001' AND DiseaseID__c < 'GARD:0000010' ORDER BY DiseaseID__c"
    q = API['query']+"?q=SELECT DiseaseID__c, (SELECT Feature__r.HPO_Feature_Type__c FROM GARD_Disease_Features__r) FROM GARD_Disease__c where Id='a05t0000001XKBfAAO'"
    #q = API['objects']
    r = requests.get(q, headers = headers)
    if 200 == r.status_code:
        print (json.dumps(r.json(), indent=2))
    else:
        print ('%s: returns status code %d\n%s' % (
            API['browse'], r.status_code, json.dumps(r.json(),indent=2)),
               file=sys.stderr)

class DiseaseSummary:
    def __init__(self):
        self.headers = {
            'Authorization': 'Bearer %s' % TOKEN
        }
        
        q = API['query']+'?q=SELECT COUNT() FROM GARD_Disease__c WHERE DiseaseID__c != NULL'
        r = requests.get(q, headers = self.headers)
        mesg = r.json()
        
        self.diseases = dict()
        if 200 != r.status_code:
            print("** can't get disease count: %d\n%s" % (
                r.status_code, json.dumps(mesg, indent=2)))
            return

        total = mesg['totalSize']
        print ('fetching %d diseases...' % total, file=sys.stderr)
        id = cnt = 0
        batch = 200
        while cnt < total:
            q = API['query']+"""?q=SELECT Id,DiseaseID__c 
FROM GARD_Disease__c 
WHERE DiseaseID__c >= 'GARD:%07d' AND DiseaseID__c < 'GARD:%07d' 
ORDER BY DiseaseID__c""" % (id, id+batch)
            #print(q)
            r = requests.get(q, headers = self.headers)
            data = r.json()
            if 200 == r.status_code:
                cnt = cnt + data['totalSize']
                for d in data['records']:
                    self.diseases[d['Id']] = d['DiseaseID__c']
            else:
                print('** error: query "%s" returns %d!' % (
                    q, r.status_code))
                break
            id = id + batch

        #print(json.dumps(diseases, indent=2))
        if len(self.diseases) != cnt:
            print('** warning: expecting %d disease(s) but instead got %d!' % (
                cnt, len(self.diseases)), file=sys.stderr)

    def count_phenotype_types(self):
        types = dict()
        for (id, curie) in self.diseases.items():
            q = API['query']+"""?q=SELECT DiseaseID__c, 
(SELECT Feature__r.HPO_Feature_Type__c FROM GARD_Disease_Features__r) 
FROM GARD_Disease__c where Id='%s'""" % id
            r = requests.get(q, headers = self.headers)
            data = r.json()['records'][0]
            if 200 == r.status_code:
                print('%s: %s' % (curie, json.dumps(data, indent=2)))
                features = data['GARD_Disease_Features__r']
                if None != features:
                    for f in features['records']:
                        for t in f['Feature__r']['HPO_Feature_Type__c'].split(';'):
                            if t not in types:
                                types[t] = set()
                            types[t].add(curie)
        for (t, curies) in types.items():
            print('%s: %d' % (t, len(curies)))

def query(q):
    headers = {
        'Authorization': 'Bearer %s' % TOKEN
    }
    print('Query: %s...' % q)    
    r = requests.get(API['query'], params={'q': q},  headers = headers)
    if 200 == r.status_code:
        print (json.dumps(r.json(), indent=2))
    else:
        print ('%s: returns status code %d' % (
            API['query'], r.status_code), file=sys.stderr)
    
class TypeAction(argparse.Action):
    def __call__(self, parser, namespace, value, opts = None):
        setattr(namespace, self.dest, value)        
        print('value => %r opts => %r namespace => %r' % (
            value, opts, namespace))
        if 'browse' != value and len(namespace.ARG) == 0:
            parser.print_help()
            print('%s: error: the following arguments are required: ARG'
                  % parser.prog)
            sys.exit(1)

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True,
                        help='Required configuration in json format')
    parser.add_argument('-t', '--type', required=True,
                        choices=['gene', 'phenotype', 'disease',
                                 'drug', 'term', 'browse', 'query'],
#                        action = TypeAction,
                        help='Input argument is of specific type')
    parser.add_argument('-r', '--reload', help='Reload disease file',
                        action='store_true')
    parser.add_argument('-i', '--include', dest='curies', default='',
                        nargs='+', help='Include only those curies specified')
    parser.add_argument('-q', '--input', help='Path to previously loaded files')
    parser.add_argument('-p', '--patch', help='Patch disease based on previously load files per the location specified by -q', action='store_true')
    parser.add_argument('-m', '--mapping', default='gard_diseases_sf.csv',
                        help='🙄 Salesforce disease mapping file (default: gard_diseases_sf.csv)')
    parser.add_argument('-s', '--skip', default=0, type=int,
                        help='Skip specified number of records in load')
    parser.add_argument('-o', '--output', default='.',
                        help='Output path (default .)')
    parser.add_argument('-u', '--update', action='store_true', default=True,
                        help='Update diseases via syncing with salesforce API (preferred)')
    parser.add_argument('ARG', nargs='?', default=[],
                        help="Input arguments either as terms or file")
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    print('-- type: %s' % args.type, file=sys.stderr)
    print('-- config: %s' % args.config, file=sys.stderr)
    print('-- output: %s' % args.output, file=sys.stderr)
    print('-- curies: %s' % args.curies, file=sys.stderr)
    print('-- ARG: %s' % args.ARG, file=sys.stderr)
    
    config = parse_config(args.config)
    API = config['api']
    TOKEN = get_auth_token(API['token'], config['params'])
    print('-- TOKEN: %s...' % TOKEN[0:32], file=sys.stderr)
    
    try:
        os.makedirs(args.output)
    except FileExistsError:
        pass

    def load_mappings(lut, file):
        print('-- mapping: %s' % file, file=sys.stderr)
        with open(file) as f:
            f.readline() # skip header
            for line in f.readlines():
                gard, sfid = line.strip().split(',')
                lut[gard] = sfid
            print('-- %d mappings loaded!' % (len(lut)), file=sys.stderr)
        
    if args.type == 'disease':
        if args.update:
            file = args.ARG
            pos = file.index('.')
            out = (file[0:pos] if pos > 0 else '') + '.out'
            err = (file[0:pos] if pos > 0 else '') + '.err'
            print('-- updating...%s => output...%s error...%s' % (
                file, out, err), file=sys.stderr)
            update_diseases(file, out, err, args.curies)
        elif args.reload:
            print('-- reloading...%s' % args.ARG[0], file=sys.stderr)
            reload_disease(args.ARG)
        elif args.patch:
            load_mappings(LUT, args.mapping)            
            print('-- patching...%s, input=%s output=%s'
                  % (args.ARG[0], args.input, args.output), file=sys.stderr)
            patch_diseases(args.ARG, args.input, args.output, args.curies)
        else:
            load_mappings(LUT, args.mapping)
            load_diseases(args.ARG, args.output,
                          include=args.curies, skip=args.skip)
    elif args.type == 'gene':
        load_genes(args.ARG, args.output)
    elif args.type == 'drug':
        load_drugs(args.ARG, args.output)
    elif args.type == 'phenotype':
        load_phenotypes(args.ARG, args.output)
    elif args.type == 'term':
        fetch_term(args.ARG)
    elif args.type == 'browse':
        #browse()
        ds = DiseaseSummary()
        ds.count_phenotype_types()
    elif args.type == 'query':
        query(args.ARG)
