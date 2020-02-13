import mysql.connector
import sys
import csv

def connect_tcrd(db):
    return mysql.connector.connect(user='tcrd', host='tcrd.ncats.io',
                                   database=db, buffered = True)

SALTS = [
    "acetate",
    "disodium",
    "sulfate",
    "sodium",
    "phosphate",
    "hcl",
    "mesylate",
    "patch",
    "hydrochloride",
    "acetate",
    "for",
    "granules",
    "ready",
    "citrate"
]

QUERY = """select b.name,c.uniprot,c.sym,b.fam
from drug_activity a, target b, protein c, t2tc d 
where drug = %(name)s and a.target_id = b.id and b.tdl='Tclin' 
and c.id=d.protein_id and b.id = d.target_id
"""

def execsearch (cursor, name):
    cursor.execute(QUERY, {'name': name})
    found = False
    if cursor.rowcount > 0:
        line = '\t'.join(row)
        for r in cursor:
            fam = r['fam']
            if fam == None:
                fam = 'Unknown'
            print ('%s\t%s\t%s\t%s\t%s' % (line, r['sym'],
                                           r['uniprot'],
                                           fam, r['name']))
        found = True
    return found

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print ('usage: %s OOPD_UNII.txt' % sys.argv[0])
        sys.exit(1)
        
    tcrd = connect_tcrd('tcrd610')
    cursor = tcrd.cursor(dictionary=True, buffered=True)
    with open(sys.argv[1], encoding='mac_roman') as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"')
        header = []
        for row in reader:
            if len(header) == 0:
                header = row
                print ('%s\tGene\tUniProt\tFamily\tName' % '\t'.join(row))
            else:
                name = row[0].lower()
                if not execsearch (cursor, name):
                    token = ''
                    for s in SALTS:
                        pos = name.find(s)
                        if pos > 0:
                            token = name[0:pos]
                            break
                    found = False
                    if len(token) > 0:
                        #print ('********** %s' % token)
                        found = execsearch (cursor, token)
                    else:
                        pos = name.find(" and")
                        if pos > 0:
                            tok1 = name[0:pos+1].strip()
                            tok2 = name[pos+4:].strip()
                            f1 = execsearch (cursor, tok1)
                            f2 = execsearch (cursor, tok2)
                            found = f1 or f2

                    if not found:
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append('')
                        print ('\t'.join(row))

    cursor.close()
    tcrd.close()
