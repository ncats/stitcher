def xmlfind(dom, path, attrib=None):
    try:
        if attrib:
            return dom.find(path).attrib[attrib]
        else:
            return dom.find(path).text.replace('\t', ' ').replace('\n', ' ').replace('"', '')
    except:
        return ""


_mesh_sup = dict()

def prepMeSH():

    # ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/
    # https://www.nlm.nih.gov/mesh/xmlconvert_ascii.html
    # https://www.nlm.nih.gov/mesh/dtype.html
    # https://www.nlm.nih.gov/mesh/xml_data_elements.html
    # https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/MSH/stats.html
    if len(_mesh_sup.keys()) > 0:
        return _mesh_sup

    mitems = ['NM', 'RN', 'SO', 'N1', 'NO', 'UI']
    mmaps = dict()
    mmaps['MH'] = 'NM'
    mmaps['PRINT ENTRY'] = 'SY'
    mmaps['ENTRY'] = 'SY'
    mmaps['MS'] = 'SO'
    mlists = ['HM', 'RR', 'SY']
    files = ['../stitcher-inputs/temp/c.bin', '../stitcher-inputs/temp/c.bin']
    for filename in files:
        fp = open(filename)
        line = fp.readline()
        while line != '':
            record = dict()
            hm = []
            while line != '\n':
                sline = line[0:-1].split (' = ')
                if len(sline) > 1 and sline[1].find('|') > -1:
                    sline[1] = sline[1].split('|')[0]
                if sline[0] in mmaps.keys():
                    sline[0] = mmaps[sline[0]]
                if sline[0] in mitems:
                    record[sline[0]] = sline[1]
                if sline[0] in mlists:
                    if sline[0] not in record.keys():
                        record[sline[0]] = []
                    record[sline[0]].append(sline[1])
                line = fp.readline()
            _mesh_sup[record['UI']] = record
            line = fp.readline()
        fp.close()
    print("Loaded mesh supplemental concept mappings")

    return _mesh_sup

def month_dictionary():
    Mon = dict()
    Mon['Jan'] = '01'
    Mon['Feb'] = '02'
    Mon['Mar'] = '03'
    Mon['Apr'] = '04'
    Mon['May'] = '05'
    Mon['Jun'] = '06'
    Mon['Jul'] = '07'
    Mon['Aug'] = '08'
    Mon['Sep'] = '09'
    Mon['Oct'] = '10'
    Mon['Nov'] = '11'
    Mon['Dec'] = '12'
    Mon['January'] = '01'
    Mon['February'] = '02'
    Mon['March'] = '03'
    Mon['April'] = '04'
    Mon['May'] = '05'
    Mon['June'] = '06'
    Mon['July'] = '07'
    Mon['August'] = '08'
    Mon['September'] = '09'
    Mon['October'] = '10'
    Mon['November'] = '11'
    Mon['December'] = '12'
    return Mon