import json
import sys
import hashlib

conditions = [
    'ConditionName',
    'ConditionUri',
    'ConditionComment',
    'isConditionDoImprecise',
    'ConditionDoId',
    'ConditionDoValue',
    'isConditionMeshImprecise',
    'ConditionMeshId',
    'ConditionMeshValue',
    'ConditionManualProductName',
    'ConditionProductName',
    'ConditionProductDate',
    'HighestPhase',
    'HighestPhaseUri',
    'HighestPhaseComment',
    'ConditionFdaUse',
    'FdaUseURI',
    'FdaUseComment',
    'OfflabelUse',
    'OfflabelUseUri',
    'OfflabelUseComment',
    'ClinicalTrial',
    'TreatmentModality'
    ]

targets = [
    'PrimaryTargetId',
    'PrimaryTargetLabel',
    'PrimaryTargetType',
    'PrimaryTargetUri',
    'PrimaryTargetGeneId',
    'PrimaryTargetGeneSymbol',
    'PrimaryTargetOrganism',
    'PrimaryTargetComment',
    'PrimaryPotencyType',
    'PrimaryPotencyValue',
    'PrimaryPotencyDimensions',
    'PrimaryPotencyTypeOther',
    'PrimaryPotencyUri',
    'PrimaryPotencyComment',
    'TargetPharmacology'
    ]

def rancho (file):
    import csv
    with open (file, 'rb') as file:
        reader = csv.reader(file,dialect='excel-tab')
        header = reader.next()
        line = 0
        p = None
        print '[',
        for row in reader:
            #print '--- record {0} ---'.format(line)
            d = {}
            c = {} # condition
            t = {} # target
            hc = hashlib.new('sha1')
            ht = hashlib.new('sha1')
            for i in range(0,len(header)):
                name = header[i]
                    
                if i < len(row):
                    r = row[i].strip()
                    if r != '':
                        x = d
                        if name in conditions:
                            x = c
                            hc.update(name)
                        elif name in targets:
                            x = t
                            ht.update(name)
                        
                        vals = r.split('|')
                        if len(vals) > 1:
                            numeric = []
                            others = []
                            for v in vals:
                                v = v.strip()
                                if v != '':
                                    if name in conditions:
                                        hc.update(v)
                                    elif name in targets:
                                        ht.update(v)
                                    if v.isdigit():
                                        numeric.append(long(v))
                                    else:
                                        others.append(v)
                            if len(numeric) == len(vals):
                                x[name] = numeric
                            else:
                                x[name] = others
                        elif name == 'Unii':
                            unii = []
                            for v in r.split(';'):
                                unii.append(v.strip())
                            if len(unii) > 1:
                                x[name] = unii
                            else:
                                x[name] = r
                        else:
                            if r.lower() == 'false':
                                x[name] = False
                            elif r.lower() == 'true':
                                x[name] = True
                            elif r.isdigit():
                                x[name] = long(r)
                            elif r.lower() != 'null' and r.lower() != 'unknown':
                                x[name] = r
                                
                            if name in conditions:
                                hc.update(r)
                            elif name in targets:
                                ht.update(r)

            if not d.has_key('CompoundName'):
                sys.stderr.write(('{0}: No CompoundName: '+'|'.join(row)+'\n').format(line))
                continue
            
            if c.has_key('ConditionName') or c.has_key('ConditionMeshValue') or c.has_key('ConditionDoValue'):
                c['id'] = hc.hexdigest()[0:10]
                d['Conditions'] = [c]
            if t.has_key('PrimaryTargetId'):
                t['id'] = ht.hexdigest()[0:10]
                d['Targets'] = [t]
                
            if d['Connected'] and len(t) > 0:
                c['TargetRefs'] = [t['id']]
                t['ConditionRefs'] = [c['id']]

            del d['Connected']
            #print json.dumps(d, indent=4, separators=(',',': '))
            
            if p != None:
                if p['CompoundName'] == d['CompoundName']:
                    if p.has_key('Conditions') and c.has_key('id'):
                        for pc in p['Conditions']:
                            if pc['id'] == c['id']:
                                if c.has_key('TargetRefs'):
                                    if pc.has_key('TargetRefs'):
                                        pc['TargetRefs'].append(c['TargetRefs'][0])
                                    else:
                                        pc['TargetRefs'] = c['TargetRefs']
                                c = None
                                break
                        if c != None:
                            p['Conditions'].append(c)
                            
                    if p.has_key('Targets') and t.has_key('id'):
                        for pt in p['Targets']:
                            if pt['id'] == t['id']:
                                if t.has_key('ConditionRefs'):
                                    if pt.has_key('ConditionRefs'):
                                        pt['ConditionRefs'].append(t['ConditionRefs'][0])
                                    else:
                                        pt['ConditionRefs'] = t['ConditionRefs']
                                t = None
                                break
                        if t != None:
                            p['Targets'].append(t)
                    d = p
                else:
                    print json.dumps(p, indent=4, separators=(',',': '))+','

            line = line + 1
            p = d
            
        if p != None:
            print json.dumps(p, indent=4, separators=(',',': '))
        print ']'
            
if __name__ == "__main__":
    import sys
    rancho(sys.argv[1])
