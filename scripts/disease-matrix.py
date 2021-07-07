from neo4j import GraphDatabase
from upsetplot import plot, generate_counts, from_memberships
from matplotlib import pyplot
import json, sys

uri = "bolt://disease.ncats.io:80"
driver = GraphDatabase.driver(uri, auth=("neo4j", ""))

def run_query(tx, query, s1, s2):
    d = {}
    for row in tx.run(query):
        #print ('%s=%d %s=%d\n' % (s1, row[s1], s2, row[s2]))
        d[s1] = row[s1]
        d[s2] = row[s2]
    return d

_data_sources = {
    'S_GARD': {
        'cons': '{X}.is_rare=true',
        'name': 'GARD'
    },
    'S_DOID': {
        'name': 'DO'
    },
    'S_ORDO_ORPHANET': {
        'name': 'Orphanet',
        'cons': 'not exists({X}.symbol) and not exists({X}.reason_for_obsolescence)'
    },
    'S_MEDGEN': {
        'name': 'MedGen',
        'labels': ['T047']
    },
    'S_OMIM': {
        'name': 'OMIM',
        'labels': ['T047']
    },    
}

data_sources = {
    'S_GARD': {
        'cons': '{X}.is_rare=true',
        'name': 'GARD'
    },
    'S_DOID': {
        'name': 'DO'
    },
    'S_ORDO_ORPHANET': {
        'name': 'Orphanet',
        'cons': 'not exists({X}.symbol) and not exists({X}.reason_for_obsolescence)'
    },
#    'S_GHR': {
#        'name': 'GHR'
#    },
#    'S_HP': {
#        'name': 'HPO'
#    },
    'S_ICD10CM': {
        'name': 'ICD-10',
        'labels': ['T047']
    },
    'S_MEDGEN': {
        'name': 'MedGen',
        'labels': ['T047']
    },
#    'S_MEDLINEPLUS': {
#       'name': 'MedlinePlus',
#        'labels': ['T047']
#    },
    'S_MESH': {
        'name': 'MeSH',
        'labels': ['T047']
    },
    'S_MONDO': {
        'name': 'MONDO',
        'cons': 'exists({X}.label)'
    },
#    'S_NORD': {
#        'name': 'NORD'
#    },
    'S_OMIM': {
        'name': 'OMIM',
        'labels': ['T047']
    },
    'S_THESAURUS': {
        'name': 'NCIT',
        'labels': ['Disease or Syndrome']
    }
}

def disease_matrix (session):
    ds = list(data_sources.keys())
    mat = {}
    series = []
    data = []

    # DO+Orphanet+OMIM
    series.append([data_sources['S_DOID']['name'],
                   data_sources['S_OMIM']['name'],
                   data_sources['S_ORDO_ORPHANET']['name']])
    for r in session.run("""
match (n:S_DOID)-[e1:N_Name|:I_CODE]-(m:S_OMIM:T047)-[e2:N_Name|:I_CODE]-(o:S_ORDO_ORPHANET)-[:N_Name|:I_CODE]-(n) return count(distinct n) as DO,count(distinct m) as OMIM, count(distinct o) as ORPHANET"""):
        data.append((r['DO'] + r['OMIM'] + r['ORPHANET'])/3)

    # GARD+Orphanet+OMIM
    series.append([data_sources['S_GARD']['name'],
                   data_sources['S_OMIM']['name'],
                   data_sources['S_ORDO_ORPHANET']['name']])
    for r in session.run("""
match (n:S_GARD)-[e1:N_Name|:I_CODE]-(m:S_OMIM:T047)-[e2:N_Name|:I_CODE]-(o:S_ORDO_ORPHANET)-[:N_Name|:I_CODE]-(n) return count(distinct n) as GARD,count(distinct m) as OMIM, count(distinct o) as ORPHANET"""):
        data.append((r['GARD'] + r['OMIM'] + r['ORPHANET'])/3)

    # DO+Orphanet+MedGen
    series.append([data_sources['S_DOID']['name'],
                   data_sources['S_MEDGEN']['name'],
                   data_sources['S_ORDO_ORPHANET']['name']])
    for r in session.run("""
match (n:S_DOID)-[e1:N_Name|:I_CODE]-(m:S_MEDGEN:T047)-[e2:N_Name|:I_CODE]-(o:S_ORDO_ORPHANET)-[:N_Name|:I_CODE]-(n) return count(distinct n) as DO,count(distinct m) as MEDGEN, count(distinct o) as ORPHANET"""):
        data.append((r['DO'] + r['MEDGEN'] + r['ORPHANET'])/3)

    # GARD+Orphanet+MONDO
    series.append([data_sources['S_GARD']['name'],
                   data_sources['S_MONDO']['name'],
                   data_sources['S_ORDO_ORPHANET']['name']])
    for r in session.run("""
match (n:S_GARD)-[e1:N_Name|:I_CODE]-(m:S_MONDO)-[e2:N_Name|:I_CODE]-(o:S_ORDO_ORPHANET)-[:N_Name|:I_CODE]-(n) return count(distinct n) as GARD,count(distinct m) as MONDO, count(distinct o) as ORPHANET"""):
        data.append((r['GARD'] + r['MONDO'] + r['ORPHANET'])/3)
    
    for i in range (0, len(ds)):
        s1 = ds[i]
        query = 'match (a:DATA)-->(n:`%s`' % s1
        ds1 = data_sources[s1]
        if 'labels' in ds1:
            for l in ds1['labels']:
                query += ':`%s`' % l
        query += ')-[:N_Name|:I_CODE*1]-(m:`'
        for j in range (i+1, len(ds)):
            s2 = ds[j]
            q = query+s2+'`'
            ds2 = data_sources[s2]
            if 'labels' in ds2:
                for l in ds2['labels']:
                    q += ':`%s`' % l
            q += ')<--(b:DATA)'
            q += ' where not a:TRANSIENT and not b:TRANSIENT'
            if 'cons' in ds1:
                q += ' and '+ds1['cons'].format(X='a')
            if 'cons' in ds2:
                q += ' and '+ds2['cons'].format(X='b')
            q += (' return count(distinct n) as `%s`, count(distinct m) as `%s`'
                  % (s1, s2))
            print('executing ==> %s' % q, file=sys.stderr)
            d = session.read_transaction(run_query, q, s1, s2)
            if ds1['name'] not in mat:
                mat[ds1['name']] = {}
            if ds2['name'] not in mat:
                mat[ds2['name']] = {}
            mat[ds1['name']][ds2['name']] = d[s1]
            mat[ds2['name']][ds1['name']] = d[s2]
            series.append([ds1['name'], ds2['name']])
            data.append((d[s1] + d[s2])/2)
            #print(json.dumps(d, indent=2))
        query = 'match (a:`%s`' % s1
        if 'labels' in ds1:
            for l in ds1['labels']:
                query += ':`%s`' % l
        query += ')<-[:PAYLOAD]-(d) where not a:TRANSIENT'
        if 'cons' in ds1:
            query += ' and ' + ds1['cons'].format(X='d')
        for r in session.run(query+' return count(distinct a) as count'):
            series.append([ds1['name']])
            data.append(r['count'])

    D = from_memberships(series, data=data)
    plot(D)#, orientation='vertical', show_counts='%d')
    pyplot.show()
    
    return mat

if __name__ == '__main__':
    with driver.session() as session:
        D = disease_matrix(session)
        print(json.dumps(D, indent=2))
    driver.close()
