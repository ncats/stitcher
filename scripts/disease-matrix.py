from neo4j import GraphDatabase

uri = "bolt://disease.ncats.io:80"
driver = GraphDatabase.driver(uri, auth=("neo4j", ""))

def run_query(tx, query, s1, s2):
    for row in tx.run(query):
        print ('%s=%d %s=%d\n' % (s1, row[s1], s2, row[s2]))

data_sources = {
    'S_GARD': {
        'cons': '{X}.is_rare=true'
    },
    'S_DOID': {},
    'S_ORDO_ORPHANET': {
        'cons': 'not exists({X}.symbol) and not exists({X}.reason_for_obsolescence)'
    },
    'S_GHR': {},
    'S_HP': {},
    'S_ICD10CM': {},
    'S_MEDGEN': {
        'labels': ['T047']
    },
    'S_MEDLINEPLUS': {
        'labels': ['T047']
    },
    'S_MESH': {
        'labels': ['T047']
    },
    'S_MONDO': {
        'cons': 'exists({X}.label)'
    },
    'S_NORD': {},
    'S_OMIM': {
        'labels': ['T047']
    },
    'S_THESAURUS': {
        'labels': ['Disease or Syndrome']
    }
}

def disease_matrix (session):
    ds = list(data_sources.keys())
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
            if 'cons' in ds1 or 'cons' in ds2:
                q += ' where'
            joint = ''
            if 'cons' in ds1:
                q += ' '+ds1['cons'].format(X='a')
                joint = ' and'
            if 'cons' in ds2:
                q += joint +' '+ds2['cons'].format(X='b')
            q += (' return count(distinct n) as `%s`, count(distinct m) as `%s`'
                  % (s1, s2))
            print('executing ==> %s' % q)
            session.read_transaction(run_query, q, s1, s2)

if __name__ == '__main__':
    with driver.session() as session:
        disease_matrix(session)
    driver.close()
