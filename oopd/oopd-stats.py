import json
import sys

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print ('usage: %s JSON' % sys.argv[0])
        sys.exit(1)
    unmapped = 0
    approved = 0
    total = 0
    gard = {}
    with open (sys.argv[1]) as f:
        data = json.load(f)
        total = len(data)
        for d in data:
            status = d['Orphan Drug Status']
            isapproved = status.find('Approved') > 0
            if isapproved:
                approved += 1
            if 'DesignationMapped' in d:
                for str, vals in d['DesignationMapped'].items():
                    for v in vals:
                        if 'mapping' in v:
                            isgard = False
                            orpha = []
                            for x in v['mapping']:
                                if x.startswith('GARD:'):
                                    gard[x] = isapproved
                                    isgard = True
                                elif x.startswith('ORPHA:'):
                                    orpha.append(x)
                            if isapproved and len(orpha) > 0 and not isgard:
                                print ('** %s but no GARD found!' % orpha)
    print ('# unmapped = %d, approved = %d, total = %d'
           % (unmapped, approved, total))
    gard_approved = list(filter(lambda x: gard[x] == 1, gard.keys()))
    print ('GARD approved...%d\n%s' % (len(gard_approved),gard_approved))
    
