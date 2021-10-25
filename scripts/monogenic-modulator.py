import sys, json
import logging as logger

logger.basicConfig(level=logger.INFO)

M = {}
KEYWORDS = ['hyper', 'hypo', 'increased', 'decreased', 'prolonged', 'shortened']

def paths_to_dict(paths):
    pd = dict()
    for p in paths:
        for i,n in enumerate(p):
            if n not in pd or pd[n] < i:
                pd[n] = i
    return pd
        
def semantic_sim(paths, term1, term2):
    sim = -1.
    lca = set() # lowest common ancestor
    if term1 not in paths:
        logger.error('Unknown term: %s' % term1)
    elif term2 not in paths:
        logger.error('Unknown term: %s' % term2)
    else:
        p1 = paths_to_dict(paths[term1])
        #logger.info('>>> %s: %s' % (term1, p1))
        p2 = paths_to_dict(paths[term2])
        #logger.info('>>> %s: %s' % (term2, p2))        
        lca = list(p1.keys() & p2.keys())
        lca.sort(key=p1.__getitem__)
        #logger.info('+++ sorted.. %s' % loa)
        sim = float(len(lca) / len(p1 | p2))
    return (sim, lca)

def hpo_semantic_sim(term1, term2):
    return semantic_sim(M['hp_paths'], term1, term2)

def main1(argv):
    if len(argv) < 3:
        print('usage: %s monogenic.json GENES...' % argv[0])
        sys.exit(1)

    with open(argv[1]) as f:
        M = json.load(f)
        if 'genes' not in M or 'diseases' not in M or 'phenotypes' not in M:
            logger.error('%s: not a valid monogenic JSON file!' % argv[1])
        else:
            G = M['genes']
            D = M['diseases']
            P = M['phenotypes']
            A = M['go_annotations']
            X = {}
            for i in range(2, len(argv)):
                gene = argv[i]
                print('%s...' % gene)
                if gene in G:
                    X[gene] = {
                        'gene': G[gene],
                        'phenotypes': {},
                    }
                    phenotypes = {}
                    for t, d in G[gene]['diseases'].items():
                        print('...%s %s' % (t, d))
                        if t in D:
                            for k, p in D[t]['phenotypes'].items():
                                if k not in phenotypes:
                                    phenotypes[k] = {
                                        'label': p['label'],
                                        'sim': {},
                                        'count': 1
                                    }
                                else:
                                    phenotypes[k]['count'] += 1
                                    
                                for x,y in phenotypes.items():
                                    (val, lca) = hpo_semantic_sim(k, x)
                                    y['sim'][k] = {
                                        'label': P[k]['label'],
                                        'value': val,
                                        'lca': lca
                                    }
                                        
                    X[gene]['phenotypes'] = phenotypes
                    #print('...phenotypes\n%s' % json.dumps(
                    #    X, indent=2))
                    with open(gene+'.json', 'w') as file:
                          print(json.dumps(X, indent=2), file=file)
            
            print('%d genes\n%d diseases\n%d phenotypes' % (
                len(G), len(D), len(P)))

def LoF(gene):
    return ('Disease-causing germline mutation(s) (loss of function) in'
            in gene['functions'])

def GoF(gene):
    return ('Disease-causing germline mutation(s) (gain of function) in'
            in gene['functions'])

def phenotype_paths(M, n):
    for path in M['hp_paths'][n]:
        for k, v in enumerate(path):
            for i in range(0, k):
                print(' ', end='')
            print(' %s: %s' % (v, M['phenotypes'][v]['label']))
        print('')

def get_best_path (paths, terms, root):
    max = 0.
    max_lca = []
    max_t1, max_t2 = (None, None)
    for i in range(0, len(terms)):
        t1 = terms[i]
        for j in range(0, i):
            t2 = terms[j]
            (sim, lca) = semantic_sim(paths, t1, t2)
            if sim > max and lca[-1] == root:
                max = sim
                max_lca = lca
                max_t1 = t1
                max_t2 = t2
    return (max, max_t1, max_t2, max_lca)

def print_path(lca, vocab):
    for k,v in enumerate(lca):
        for i in range(0, k):
            print(' ', end='')
        print(' %s: %s' % (v, vocab[v]['label']))
    

def check_gene(M, gene, data):
    lof = {}
    gof = {}
    all = {}
    for k, d in data['diseases'].items():
        for t in d['types']:
            if (t ==
                'Disease-causing germline mutation(s) (loss of function) in'):
                lof[k] = d
                #all[k] = d
            elif (t ==
                  'Disease-causing germline mutation(s) (gain of function) in'):
                gof[k] = d
                #all[k] = d
            elif t == 'Disease-causing germline mutation(s) in':
                all[k] = d

    ok = False
    #if len(lof) > 0 and len(gof) > 0:
    #if len(data['diseases']) > 1:
    if len(all) > 0: # either lof or gof..
        ok = True
        print('############# %s %s #############' % (gene, data['tdl']))
        goannos = []
        for k,a in data['go_annotations'].items():
            #print(' %s: %s' % (k, a['label']))
            goannos.append(k)

        # cellular_component            
        (max, max_g1, max_g2, max_lca) = get_best_path(
            M['go_paths'], goannos, 'GO:0005575')
        #if max > 0.:
        #    print('++ %s :: %s = %.3f' % (max_g1, max_g2, max))
        #    print_path(max_lca, M['go_annotations'])

        # biological_process
        (max, max_g1, max_g2, max_lca) = get_best_path(
            M['go_paths'], goannos, 'GO:0008150') 
        #if max > 0.:
        #    print('++ %s :: %s = %.3f' % (max_g1, max_g2, max))
        #    print_path(max_lca, M['go_annotations'])

        # molecular_function
        (max, max_g1, max_g2, max_lca) = get_best_path(
            M['go_paths'], goannos, 'GO:0003674') 
        #if max > 0.:
        #    print('++ %s :: %s = %.3f' % (max_g1, max_g2, max))
        #    print_path(max_lca, M['go_annotations'])
        
        diseases = list(all.keys())
        ancestors = {}
        for i in range(0, len(diseases)):
            k1 = diseases[i]
            d1 = M['diseases'][k1]
            if k1 in lof and k1 in gof:
                f1 = 'LoF/GoF'
            else:
                f1 = 'LoF' if k1 in lof else 'GoF'
            for j in range(0, i+1):
                k2 = diseases[j]
                d2 = M['diseases'][k2]
                if k2 in lof and k2 in gof:
                    f2 = 'LoF/GoF'
                else:
                    f2 = 'LoF' if k2 in lof else 'GoF'
                if 'phenotypes' in d1 and 'phenotypes' in d2:
                    print('..%s (%s) :: %s (%s)' % (k1, f1, k2, f2))
                    p = set(d1['phenotypes'].keys())
                    ov = p.intersection(d2['phenotypes'].keys())
                    for n in ov:
                        #phenotype_paths(M, n)
                        print('  %s: %s' % (n, M['phenotypes'][n]['label']))
                        
                    p1 = set(d1['phenotypes']).difference(
                        d2['phenotypes'].keys())
                    p2 = set(d2['phenotypes']).difference(
                        d1['phenotypes'].keys())

                    if len(p1) == 0 and len(p2) == 0:
                        p1 = p2 = ov
                    elif len(p1) == 0:
                        p1 = p2
                    elif len(p2) == 0:
                        p2 = p1
                        
                    max = 0.
                    max_lca = []
                    all_lca = []
                    max_p1, max_p2 = (None, None)
                    if k1 != k2:
                        print('..%s (%s)' % (k1, f1))
                    for n1 in p1:
                        if k1 != k2:
                            print('  %s: %s' % (
                                n1, M['phenotypes'][n1]['label']))
                        for n2 in p2:
                            if n1 != n2:
                                (sim, lca) = semantic_sim(M['hp_paths'], n1, n2)
                                if sim > max:
                                    max = sim
                                    max_lca = lca
                                    max_p1 = n1
                                    max_p2 = n2
                                all_lca.append((n1, n2, sim, lca))
                                ancestor = None
                                if lca[0] not in ancestors:
                                    ancestors[lca[0]] = ancestor = set ()
                                else:
                                    ancestor = ancestors[lca[0]]
                                ancestor.add(n1)
                                ancestor.add(n2)
                                
                    if k1 != k2:
                        print('..%s (%s)' % (k2, f2))
                        for n2 in p2:
                            print('  %s: %s' % (
                                n2, M['phenotypes'][n2]['label']))

                    if max_p1 != None:
                        print('++ %s :: %s = %.3f' % (max_p1, max_p2, max))
                        print_path(max_lca, M['phenotypes'])
                        
                    #all_lca.sort(key=lambda x: len(x[3]), reverse=True)
                    #for j in range(0, min(5, len(all_lca))):
                    #    print('__ %s + %s = %f' % (
                    #        all_lca[j][0], all_lca[j][1], all_lca[j][2]))
                    #    print_path(all_lca[j][3], M['phenotypes'])

        ancestors.pop('HP:0000001', None)
        #ancestors.pop('HP:0000118', None)
        parents = list(ancestors.keys())
        parents.sort(key = lambda x: len(ancestors[x]), reverse=True)
        print('>>>> ancestors:')
        terms = {}
        opposite = False
        for p in parents:
            Q = ancestors[p]
            print('...%s %d: %s' % (p, len(Q), M['phenotypes'][p]['label']))
            for q in Q:
                label = M['phenotypes'][q]['label'].lower()
                marker = ''
                normalize = label
                for t in label.split('\s'):
                    for k in KEYWORDS:
                        normalize = normalize.replace(k, '')
                        if t.startswith(k):
                            marker = '$'
                if normalize == label:
                    pass
                else:
                    if normalize not in terms:
                        terms[normalize] = set()
                    terms[normalize].add(label)
                    if len(terms[normalize]) > 1:
                        opposite = True
                print('   %s %s %s' % (marker, q, label))
        if opposite:
            for t,c in terms.items():
                if len(c) > 1:
                    print('@@@ %s(%s)    %s: %s' % (gene, data['tdl'], t, c))
            
    return ok
        
def main2(argv):
    if len(argv) < 2:
        print('usage: %s monogenic.json' % argv[0])
        sys.exit(1)

    with open(argv[1]) as f:
        M = json.load(f)
        count = 0
        for sym, gene in M['genes'].items():
            if check_gene(M, sym, gene):
                count += 1
        print('######## %d matching gene(s)! ########' % count)


def main3(argv):
    if len(argv) < 4:
        print('usage: %s monogenic.json HPO1 HPO2...' % argv[0])
        sys.exit(1)

    with open(argv[1]) as f:
        M = json.load(f)
        for i in range(2, len(argv)):
            for j in range(2, i):
                print('+++ semantic similarity between %s[%d] and %s[%d]...' % (
                    argv[i], i, argv[j], j))
                (val, lca) = semantic_sim(M['hp_paths'], argv[i], argv[j])
                print('%s: %s' % (argv[i], M['phenotypes'][argv[i]]['label']))
                print('%s: %s' % (argv[j], M['phenotypes'][argv[j]]['label']))
                print('...sim: %f' % val)                
                print('...lca')
                for k,v in enumerate(lca):
                    for j in range(0, k):
                        print(' ', end='')
                    print(' %s: %s' % (v, M['phenotypes'][v]['label']))

def add_paths(paths, data):
    newdata = set()
    for d in data:
        if d in paths:
            for p in paths[d]:
                newdata.update(p)
        else:
            logger.warning('%s: paths not found!' % d)
    data.update(newdata)
            
def compare_go_annotations(file):
    with open(file) as f:
        M = json.load(f)
        with open('go-druggable.csv') as f1, open('go-monogenic.csv') as f2:
            import csv
            paths = M['go_paths']
            druggable = set()
            reader = csv.DictReader(f1)
            for r in reader:
                goid = r['ID']
                druggable.add(goid)
                #if len(druggable) > 20:
                #    break
            add_paths(paths, druggable)
            #logger.info('druggable: %s' % druggable)
            
            monogenic = set()
            reader = csv.DictReader(f2)
            for r in reader:
                goid = r['ID']
                monogenic.add(goid)
                #if len(monogenic) > 20:
                #    break
            add_paths(paths, monogenic)
            #logger.info('monogenic: %s' % monogenic)
            
            #logger.info('& = %s' % (druggable & monogenic))
            #logger.info('| = %s' % (druggable | monogenic))
            sim = float(len(druggable & monogenic)) / len(druggable|monogenic)
            print('** similarity = %f' % (sim))

def main4(argv):
    if len(argv) < 2:
        print('usage: %s monogenic.json' % argv[0])
        sys.exit(1)
        
    with open(argv[1]) as f:
        M = json.load(f)
        G = M['genes']
        lofgof = 0
        lof = 0
        gof = 0
        dcounts = {}
        for g,d in G.items():
            funcs = d['functions']
            if ('Disease-causing germline mutation(s) (loss of function) in'
                in funcs and
                'Disease-causing germline mutation(s) (gain of function) in'
                in funcs):
                lofgof += 1
            if ('Disease-causing germline mutation(s) (loss of function) in'
                  in funcs):
                lof += 1
            if ('Disease-causing germline mutation(s) (gain of function) in'
                  in funcs):
                gof += 1
            dc = len(d['diseases'])
            if dc in dcounts:
                dcounts[dc] += 1
            else:
                dcounts[dc] = 1
        print('%d genes' % len(G))
        print('lof+gof: %d' % lofgof)
        print('lof: %d' % lof)
        print('gof: %d' % gof)
        print('disease count distribution: %s' % dcounts)

if __name__ == '__main__':
    #main1(sys.argv)
#    main2(sys.argv)
    #main3(sys.argv)
    main4(sys.argv)
#    compare_go_annotations(sys.argv[1])
