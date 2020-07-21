import os
import sys
import cookielib
import urllib2
import json
import time
import resolver

chemblSite = "https://www.ebi.ac.uk/chembl/api/data/"

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

def iterateThruPages(task, func, header):
    filename = 'chembl/'+task+'.txt'

    skip = 0
    if not os.path.exists(filename):
        fp = open(filename, 'w')
        fp.write('\t'.join(header) + '\tref_id\tref_url\tref_type\n')
        fp.close()
    else:
        skip = len(open(filename).readlines()) - 1

    fp = open(filename, 'a')
    top = 20
    max = 0
    while skip < max or max == 0:
        uri = chemblSite + task + '?format=json&limit='+str(top)+'&offset='+str(skip)
        obj = requestJson(uri)
        if max == 0:
            max = obj['page_meta']['total_count']
        if not obj.has_key(task+'s'):
            newobj = dict()
            newobj[task+'s'] = []
            newobj[task+'s'].append(obj)
            obj = newobj
            skip = max
        if obj is None:
            skip = skip
        elif len(obj[task+'s']) == 0:
            skip = max
        else:
            for entry in obj[task+'s']:
                func(fp, entry, header)
        sys.stderr.write(uri+"\n")
        sys.stderr.flush()
        skip = skip + top
    fp.close()

    return

def safeAdd(string, obj, delim = ""):
    if obj is None:
        return string + delim
    try:
        obj = obj.encode('ascii', 'ignore')
    except:
        obj = str(obj)
    obj = obj.replace('\n', ' ')
    obj = obj.replace('\t', ' ')
    return string + obj + delim

def getRefType(refType):
    refTypes = ['DailyMed', 'FDA', 'ATC', 'ClinicalTrials', 'PubMed', 'Wikipedia']
    if refType in refTypes:
        return refTypes.index(refType)
    return len(refTypes)

def getSynType(refType):
    refTypes = ['FDA', 'INN', 'USAN', 'ATC', 'BAN', 'JAN', 'MI', 'USP', 'MERCK_INDEX', 'RESEARCH_CODE']
    if refType in refTypes:
        return refTypes.index(refType)
    return len(refTypes)

def drugIndicationParse(fp, entry, header):
    if 'mesh_id' not in entry or entry['mesh_id'] == "":
        print entry
        print "SFDFSD"
        sys.exit()

    oline = ''
    for item in header:
        oline = safeAdd(oline, entry[item], '\t')
    bestRef = None
    for item in entry['indication_refs']:
        if bestRef == None or getRefType(item['ref_type']) < getRefType(bestRef['ref_type']):
            bestRef = item
    if bestRef != None:
        oline = safeAdd(oline, bestRef['ref_id'], '\t')
        oline = safeAdd(oline, bestRef['ref_url'], '\t')
        oline = safeAdd(oline, bestRef['ref_type'])
        if bestRef['ref_type'] == 'ClinicalTrials':
            oline = oline + ' Phase ' + str(entry['max_phase'])
    fp.write(oline)
    fp.write('\n')

    return

def mechanismParse(fp, entry, header):
    oline = ''
    for item in header:
        oline = safeAdd(oline, entry[item], '\t')
    bestRef = None
    for item in entry['mechanism_refs']:
        if bestRef == None or getRefType(item['ref_type']) < getRefType(bestRef['ref_type']):
            bestRef = item
    if bestRef != None:
        oline = safeAdd(oline, bestRef['ref_id'], '\t')
        oline = safeAdd(oline, bestRef['ref_url'], '\t')
        oline = safeAdd(oline, bestRef['ref_type'])
        if bestRef['ref_type'] == 'ClinicalTrials':
            oline = oline + ' Phase ' + str(entry['max_phase'])
    fp.write(oline)
    fp.write('\n')

    return

def metabolismParse(fp, entry, header):
    oline = ''
    for item in header:
        oline = safeAdd(oline, entry[item], '\t')
    bestRef = None
    for item in entry['metabolism_refs']:
        if bestRef == None or getRefType(item['ref_type']) < getRefType(bestRef['ref_type']):
            bestRef = item
    if bestRef != None:
        oline = safeAdd(oline, bestRef['ref_id'], '\t')
        oline = safeAdd(oline, bestRef['ref_url'], '\t')
        oline = safeAdd(oline, bestRef['ref_type'])
        if bestRef['ref_type'] == 'ClinicalTrials':
            oline = oline + ' Phase ' + str(entry['max_phase_for_ind'])
    fp.write(oline)
    fp.write('\n')

    return

def drugParse(fp, entry, header):
    oline = ''
    for item in header:
        oline = safeAdd(oline, entry[item], '\t')
    bestRef = None
    for item in entry['molecule_synonyms']:
        if bestRef == None or getSynType(item['syn_type']) < getSynType(bestRef['syn_type']):
            bestRef = item
    if bestRef != None:
        oline = safeAdd(oline, bestRef['molecule_synonym'], '\t')
        oline = safeAdd(oline, bestRef['synonyms'], '\t')
        oline = safeAdd(oline, bestRef['syn_type'])
    fp.write(oline)
    fp.write('\n')

    return

def molParse(fp, entry, header):
    oline = ''
    for item in header:
        oline = safeAdd(oline, entry[item], '\t')
    bestRef = None
    for item in entry['molecule_synonyms']:
        if bestRef == None or getSynType(item['syn_type']) < getSynType(bestRef['syn_type']):
            bestRef = item
    if bestRef != None:
        oline = safeAdd(oline, bestRef['molecule_synonym'], '\t')
        oline = safeAdd(oline, bestRef['synonyms'], '\t')
        oline = safeAdd(oline, bestRef['syn_type'])
    canSmiles = ''
    if 'molecule_structures' in entry:
        if entry['molecule_structures'] != None and 'canonical_smiles' in entry['molecule_structures']:
            oline = oline + '\t'
            oline = safeAdd(oline, entry['molecule_structures']['canonical_smiles'])
            canSmiles = entry['molecule_structures']['canonical_smiles']

    fp.write(oline)
    fp.write('\n')

    return [bestRef['molecule_synonym'], canSmiles]

def updateFiles():
    header = ['molecule_chembl_id', 'parent_molecule_chembl_id', 'mesh_id', 'mesh_heading', 'efo_term', 'drugind_id']
    iterateThruPages('drug_indication', drugIndicationParse, header)

    header = ['molecule_chembl_id', 'target_chembl_id', 'action_type', 'mechanism_comment', 'mechanism_of_action']
    iterateThruPages('mechanism', mechanismParse, header)

    header = ['drug_chembl_id', 'substrate_chembl_id', 'metabolite_chembl_id', 'target_chembl_id', 'organism', 'substrate_name', 'metabolite_name', 'met_conversion']
    iterateThruPages('metabolism', metabolismParse, header)

    header = ['molecule_chembl_id', 'first_approval', 'first_in_class', 'usan_year', 'prodrug', 'oral', 'parenteral', 'topical', 'withdrawn_country', 'withdrawn_reason', 'withdrawn_year']
    iterateThruPages('drug', drugParse, header)

def readFileDict(filename, keyCol, valCols):
    adict = dict()

    fp = open(filename, 'r')
    header = fp.readline().strip().split('\t')
    k = header.index(keyCol)
    vs = []
    for item in valCols:
        vs.append(header.index(item))
    line = fp.readline()
    while line != "":
        sline = line[0:-1].split("\t")
        entry = []
        for i in range(len(vs)):
            if len(sline) > vs[i]:
                entry.append(sline[vs[i]])
            else:
                entry.append('')
        adict[sline[k]] = entry
        line = fp.readline()
    fp.close()

    return adict

def updateChemblMol():
    drugfile = 'chembl/drug.txt'
    molfile = 'chembl/chembl-mol.txt'

    c2Name = readFileDict(drugfile, 'molecule_chembl_id', ['ref_id'])

    header = ['molecule_chembl_id', 'first_approval', 'first_in_class', 'usan_year', 'prodrug', 'oral', 'parenteral', 'topical', 'withdrawn_country', 'withdrawn_reason', 'withdrawn_year']
    if not os.path.exists(molfile):
        fp = open(molfile, 'w')
        fp.write('\t'.join(header) + '\tref_id\tref_url\tref_type\tcan_smiles\n')
        fp.close()
    else:
        c2Name2 = readFileDict(molfile, 'molecule_chembl_id', ['ref_id', 'can_smiles'])
        for key in c2Name2:
            if key not in c2Name:
                c2Name[key] = c2Name2[key]

    files = []
    files.append(['chembl/drug_indication.txt', 'molecule_chembl_id', ['molecule_chembl_id']])
    files.append(['chembl/mechanism.txt', 'molecule_chembl_id', ['molecule_chembl_id']])
    files.append(['chembl/metabolism.txt', 'drug_chembl_id', ['drug_chembl_id']])
    files.append(['chembl/metabolism.txt', 'substrate_chembl_id', ['substrate_chembl_id']])
    files.append(['chembl/metabolism.txt', 'metabolite_chembl_id', ['metabolite_chembl_id']])
    fp = open(molfile, 'a')
    for item in files:
        entries = readFileDict(item[0], item[1], item[2])
        for key in entries:
            if key not in c2Name.keys():
                uri = 'https://www.ebi.ac.uk/chembl/api/data/molecule/'+key+'?format=json'
                sys.stderr.write(key+'\n')
                obj = requestJson(uri)
                entry = molParse(fp, obj, header)
                c2Name[key] = entry
    fp.close()

    return c2Name

if __name__=="__main__":
    # first delete files from chembl dir, then run:
    #updateFiles()

    # get ChEMBL definitions of all of the relevant molecules
    c2Name = updateChemblMol()

    # map to UNIIs
    uniiMap = dict()
    uniiMapFile = 'chembl/chembl-unii_map.txt'
    if os.path.exists(uniiMapFile):
        mapping = open(uniiMapFile, 'r').readlines()
        for line in mapping:
            sline = line.strip().split('\t')
            if sline[1] != "_N/A":
                uniiMap[sline[0]] = sline[1]

    mappingFP = open(uniiMapFile, 'a')
    #c2Name = dict()
    #c2Name['CHEMBL2107829'] = ['Emixustat HCl', 'Cl.NCC[C@@H](O)c1cccc(OCC2CCCCC2)c1']
    #c2Name['CHEMBL4297511'] = ['Firibastat']
    for key in c2Name:
        if key not in uniiMap:
            unii = resolver.resolveName(c2Name[key])
            if len(unii) != 10:
                if len(unii) < 1 and len(c2Name[key][0]) > 0 or (len(c2Name[key]) > 1 and len(c2Name[key][1]) > 0):
                    print key, c2Name[key], unii
                    #sys.exit()
                unii = "_N/A"
            else:
                uniiMap[key] = unii
            mappingFP.write(key+"\t"+unii)
            for item in c2Name[key]:
                mappingFP.write("\t"+item)
            mappingFP.write("\n")
            mappingFP.flush()
    mappingFP.close()
