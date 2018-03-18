import json
import sys
import hashlib

#check that the python version is correct (need 2)
if sys.version_info[0] > 2:
    raise "Must be using Python 2! Aborting."

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
        #line counter for debugging purposes
        line = 1
        #declare a "summary object" for one compound
        combined_comp_rows = None
        print '[',
        for row in reader:
            line += 1
            #dictionary to store current row info
            current_row_dict = {}
            c = {} # condition
            t = {} # target
            hc = hashlib.sha1()
            ht = hashlib.sha1()

            #dissect the row
            for i in range(0,len(header)):
                #get the variable/column name
                name = header[i]

                #some rows are shorter than header
                if i < len(row):
                    #get rid of white space around each element
                    r = row[i].strip()

                    #if the row element is non-empty
                    if r != '':

                        #point temp variable to row dictionary
                        x = current_row_dict

                        #if col name belongs to conditions
                        if name in conditions:
                            #point temp var to conditions
                            x = c
                            #make a hash
                            hc.update(name)
                        #likewise, for targets
                        elif name in targets:
                            x = t
                            ht.update(name)

                        #split up the observations
                        vals = r.split('|')

                        #cycle through all observations
                        if len(vals) > 1:
                            for vi in range(len(vals)):
                                #remove trailing whitespaces
                                vals[vi] = vals[vi].strip()

                                if vals[vi] != '':
                                    #convert to digit
                                    if vals[vi].isdigit():
                                        vals[vi] = long(vals[vi])

                                    #update targets'/conditions' hashes
                                    if name in conditions:
                                        hc.update(v)
                                    elif name in targets:
                                        ht.update(v)
                            #add value to corresponding variable in temp row object
                            x[name] = vals
                        elif name == 'Unii':
                            unii = []
                            for v in r.split(';'):
                                unii.append(v.strip())
                            if len(unii) > 1:
                                x[name] = unii
                            else:
                                x[name] = r
                        #replace 'curative' treatment modality with 'primary'
                        elif (name == 'TreatmentModality' and r.lower() == 'curative'):
                            x[name] = 'Primary'
                        else:
                            if r.lower() == 'false':
                                x[name] = False
                            elif r.lower() == 'true':
                                x[name] = True
                            elif r.isdigit():
                                x[name] = long(r)
                            elif r.lower() != 'null':# and r.lower() != 'unknown':
                                x[name] = r

                            #update the hashes for conditions/targets
                            #if applicable
                            if name in conditions:
                                hc.update(r)
                            elif name in targets:
                                ht.update(r)

            if not current_row_dict.has_key('CompoundName'):
                sys.stderr.write(('{0}: No CompoundName: '+'|'.join(row)+'\n').format(line))
                continue
            
            if c.has_key('ConditionName') or c.has_key('ConditionMeshValue') or c.has_key('ConditionDoValue'):
                c['id'] = hc.hexdigest()[0:10]
                current_row_dict['Conditions'] = [c]

            if t.has_key('PrimaryTargetId'):
                t['id'] = ht.hexdigest()[0:10]
                current_row_dict['Targets'] = [t]

            if current_row_dict['Connected'] and len(t) > 0:
                c['TargetRefs'] = [t['id']]
                t['ConditionRefs'] = [c['id']]

            #remove connected key (already used)
            del current_row_dict['Connected']
            #and curated by (not needed)
            if current_row_dict.has_key('CuratedBy'):
                del current_row_dict['CuratedBy']

            #if compound summary object does not exist...
            if combined_comp_rows == None:
                #point it to current row object
                combined_comp_rows = current_row_dict
            else:
                #if it exists and does not belong to the same compound as row object...
                if combined_comp_rows['CompoundName'] != current_row_dict['CompoundName']:
                    #dump json to file
                    print json.dumps(combined_comp_rows, indent=4, separators=(',', ': ')) + ','
                    #and point the summary object to new row
                    combined_comp_rows = current_row_dict
                else:
                    #if new row has information on the compound in the summary
                    #if there's a condition key 'id'...
                    if c.has_key('id'):
                        #and that key exists in the summary object...
                        if combined_comp_rows.has_key('Conditions'):
                            #append all target references from that key
                            #to every condition with the same id...
                            for pc in combined_comp_rows['Conditions']:
                                if pc['id'] == c['id']:
                                    if c.has_key('TargetRefs'):
                                        if pc.has_key('TargetRefs'):
                                            #[0] is used because that property
                                            # was deliberately made a list
                                            pc['TargetRefs'].append(c['TargetRefs'][0])
                                        else:
                                            pc['TargetRefs'] = c['TargetRefs']
                                    c = None
                                    break
                            else:
                                #and finally append the new condition
                                combined_comp_rows['Conditions'].append(c)
                        #if it's the first condition -- create it in the summary object anew
                        else:
                            combined_comp_rows['Conditions'] = [c]

                    #do the same thing for targets
                    #if there's a target key 'id'...
                    if t.has_key('id'):
                        #and that key exists in the summary object...
                        if combined_comp_rows.has_key('Targets'):
                            #append all condition references from that key
                            #to every target with the same id as current target...
                            for pt in combined_comp_rows['Targets']:
                                if pt['id'] == t['id']:
                                    if t.has_key('TargetRefs'):
                                        if pt.has_key('TargetRefs'):
                                            #[0] is used because that property
                                            # was deliberately made a list
                                            pt['TargetRefs'].append(t['TargetRefs'][0])
                                        else:
                                            pt['TargetRefs'] = t['TargetRefs']
                                    t = None
                                    break
                            else:
                                #and finally append the new target
                                combined_comp_rows['Targets'].append(t)
                        #if it's the first target -- create it in the summary object anew
                        else:
                            combined_comp_rows['Targets'] = [t]
                    #point current row object to summary object
                    current_row_dict = combined_comp_rows

    #in the end either a new row or a summary object point to the same unrecorded object
    #so it needs to be recorded
    print json.dumps(current_row_dict, indent=4, separators=(',',': '))
    print ']'


if __name__ == "__main__":
    import sys
    rancho(sys.argv[1])
