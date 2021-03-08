#!/usr/bin/python3

from openpyxl import load_workbook
import zipfile
import requests
import xml
import gzip
import os
import sys
import io
import json

_mesh_sup = dict()

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

def xmlfind(dom, path, attrib=None):
    try:
        if attrib:
            return dom.find(path).attrib[attrib]
        else:
            return dom.find(path).text.replace('\t', ' ').replace('\n', ' ').replace('"', '')
    except:
        return ""

def parseCT(zf, ctfile):
    ctnum = ctfile[12:-4]
    url = 'https://clinicaltrials.gov/ct2/show/' + ctnum
    try:
        fp = zf.open(ctfile)
    except:
        return([ctnum])
    tree = xml.etree.ElementTree.parse(fp)
    root = tree.getroot()
    #  <brief_title>A Phase I Clinical Trial of Meplazumab in Healthy Volunteer</brief_title>
    #  <overall_status>Recruiting</overall_status>
    #  <start_date type="Anticipated">April 25, 2020</start_date>
    #  <completion_date type="Anticipated">June 30, 2020</completion_date>
    #  <primary_completion_date type="Anticipated">June 1, 2020</primary_completion_date>
    #  <last_update_submitted>April 29, 2020</last_update_submitted>
    #  <phase>Phase 1</phase>
    #  <study_type>Interventional</study_type>
    #  <intervention>
    #    <intervention_type>Drug</intervention_type>
    #    <intervention_name>meplazumab for injection</intervention_name>

    title = xmlfind(root, './brief_title')
    phase = xmlfind(root, './phase')
    if phase == 'N/A':
        phase = 'Not Applicable'
    study_type = xmlfind(root, './study_type')

    # https://clinicaltrials.gov/ct2/html/images/info/public.xsd
    # <xs:simpleType name="recruitment_status_enum">
    # <xs:restriction base="xs:string">
    # <xs:enumeration value="Active, not recruiting"/>
    # <xs:enumeration value="Completed"/>
    # <xs:enumeration value="Enrolling by invitation"/>
    # <xs:enumeration value="Not yet recruiting"/>
    # <xs:enumeration value="Recruiting"/>
    # <xs:enumeration value="Suspended"/>
    # <xs:enumeration value="Terminated"/>
    # <xs:enumeration value="Withdrawn"/>
    # </xs:restriction>
    # </xs:simpleType>

    status = xmlfind(root, './overall_status')
    
    # <xs:enumeration value="Actual"/>
    # <xs:enumeration value="Anticipated"/>
    # <xs:enumeration value="Estimate"/>

    start_date = xmlfind(root, './start_date')
    if xmlfind(root, './start_date', 'type') != 'Actual':
        start_flag = xmlfind(root, './start_date', 'type')
    completion_date = xmlfind(root, './completion_date')
    if xmlfind(root, './completion_date', 'type') != 'Actual':
        completion_flag = xmlfind(root, './completion_date', 'type')
    primary_completion_date = xmlfind(root, './primary_completion_date')
    if xmlfind(root, './primary_completion_date', 'type') != 'Actual':
        completion_flag = xmlfind(root, './primary_completion_date', 'type')
    last_update_submitted = xmlfind(root, './last_update_submitted')
    alias = xmlfind(root, './id_info/nct_alias')
    condition = xmlfind(root, './condition')
    meshcondition = xmlfind(root, './condition_browse/mesh_term')

    interventions = []
    for intervention in root.findall('./intervention'):
        itype = xmlfind(intervention, './intervention_type')
        iname = xmlfind(intervention, './intervention_name')
        interventions.append(": ".join([itype, iname]))
    
    return([ctnum, url, condition, meshcondition, phase, status, study_type, start_date, completion_date, title, "; ".join(interventions)], alias)

def mapCTzip(): # This was the original method for annotating Alex's file with trial info
    drugdata = readGSRSworkbook()

    fp = open("clinicaltrials-UNIIs.txt", "w")
    header = "trial ID\tUNII\tUNII preferred term\tURL\tphase\tstatus\tstudy_type\tstart_date\tcompletion_date\ttitle\tinterventions\n"
    fp.write(header)

    zf = zipfile.ZipFile('AllPublicXML.zip') # https://clinicaltrials.gov/AllPublicXML.zip
    files = zf.namelist()

    idx = 0
    for filename in files[1:]: # ['Contents.txt', 'NCT0000xxxx/NCT00000102.xml', 'NCT0000xxxx/NCT00000104.xml', 'NCT0000xxxx/NCT00000105.xml', 'NCT0000xxxx/NCT00000106.xml', 'NCT0000xxxx/NCT00000107.xml', 'NCT0000xxxx/NCT00000108.xml' ...
        ctnum = filename[12:-4]
        if idx >= len(drugdata) or ctnum < drugdata[idx][0]:
            continue

        ctdata, alias = parseCT(zf, filename)
        print(drugdata[idx])
        #print(ctdata)
        while idx < len(drugdata) and ctdata[0] > drugdata[idx][0]:
            idx = idx + 1

        while idx < len(drugdata) and ctdata[0] == drugdata[idx][0]:
            outline = drugdata[idx][0:3]
            for item in ctdata[1:]:
                outline.append(item)
            outline[-1] = outline[-1] + "\n"
            fp.write("\t".join(outline))
            idx = idx + 1
        
        if idx > len(drugdata):
            break
    
    zf.close()

    fp.close()

    return

def indexCTzip():
    zf = zipfile.ZipFile('AllPublicXML.zip') # https://clinicaltrials.gov/AllPublicXML.zip
    files = zf.namelist()
    ctdata = dict()
    for filename in files[1:]: # ['Contents.txt', 'NCT0000xxxx/NCT00000102.xml', 'NCT0000xxxx/NCT00000104.xml', 'NCT0000xxxx/NCT00000105.xml', 'NCT0000xxxx/NCT00000106.xml', 'NCT0000xxxx/NCT00000107.xml', 'NCT0000xxxx/NCT00000108.xml' ...
        ctnum = filename[12:-4]
        print(ctnum)
        ctdata[ctnum], alias = parseCT(zf, filename)
        if alias != '':
            ctdata[alias] = ctdata[ctnum]
    zf.close()

    with gzip.open('ctdata.json.gz', 'wt') as f:
        json.dump(ctdata, f, ensure_ascii=False, indent=4)

    return

def readGSRSworkbook():

    data = []
    wb = load_workbook("usct-trial-to-substance-mapping-public-20210212.xlsx", read_only=True)
    ws = wb.active
    for row in ws.rows:
        data.append( [str(cell.value) if cell.value != None else '' for cell in row] )

    header = 0
    while data[header][0] != 'nctNumber' and header < 10:
        print("\t".join(data[header]))
        header += 1

    headers = data[header]
    print(headers)
    print("\t".join(data[header+1]))

    output = []
    for item in data[header+1:]:
        output.append(item[0:2])

    with gzip.open('ctgovuniis.json.gz', 'wt') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    return(data[header+1:])

def parsePaper(root):
    paper = dict()
    for article in root.find('./PubmedData/ArticleIdList'):
        if article.get('IdType') == "pubmed":
            paper['PMID'] = article.text
        if article.get('IdType') == "doi":
            paper['DOI'] = article.text
 
    paper['title'] = xmlfind(root, './MedlineCitation/Article/ArticleTitle')

    paper['abstract'] = paper['title']
    if root.find('./MedlineCitation/Article/Abstract') is not None:
        paper['abstract'] = ''
        for entry in root.find('./MedlineCitation/Article/Abstract'):
            if entry.text is not None:
                paper['abstract'] = paper['abstract'] + ' ' + entry.text

    phase = 'Not Applicable'
    for entry in root.find('./MedlineCitation/Article/PublicationTypeList'):
        if xmlfind(entry, '.', 'UI') == 'D017426':
            phase = 'Phase 1'
        if xmlfind(entry, '.', 'UI') == 'D017427':
            phase = 'Phase 2'
        if xmlfind(entry, '.', 'UI') == 'D017428':
            phase = 'Phase 3'
        if xmlfind(entry, '.', 'UI') == 'D017429':
            phase = 'Phase 4'
    if phase == 'Not Applicable':
        for text in [paper['title'].lower(), paper['abstract'].lower()]:
            if 'early phase 1' in text:
                phase = 'Early Phase 1'
            elif 'phase 1/phase 2' in text:
                phase = 'Phase 1/Phase 2'
            elif 'phase 1' in text:
                phase = 'Phase 1'
            elif 'phase 2/phase 3' in text:
                phase = 'Phase 2/Phase 3'
            elif 'phase 2' in text:
                phase = 'Phase 2'
            elif 'phase 3' in text:
                phase = 'Phase 3'
            elif 'phase iii' in text:
                phase = 'Phase 3'
            elif 'phase ii/iii' in text:
                phase = 'Phase 2/Phase 3'
            elif 'phase ii' in text:
                phase = 'Phase 2'
            elif 'phase i/ii' in text:
                phase = 'Phase 1/Phase 2'
            elif 'phase i' in text:
                phase = 'Phase 1'
    paper['phase'] = phase

    del paper['abstract']

    if root.find('./MedlineCitation/Article/DataBankList') is not None:
        for databank in root.find('./MedlineCitation/Article/DataBankList'):
            if xmlfind(databank, './DataBankName') == 'ClinicalTrials.gov':
                for accession in databank.find('./AccessionNumberList'):
                    paper['nct'] = accession.text

    # e.g. Therapie. Mar-Apr 1993;48(2):105-7.
    try:
        journal = root.find('./MedlineCitation/Article/Journal/ISOAbbreviation').text
    except:
        journal = root.find('./MedlineCitation/Article/Journal/Title').text
    day = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Day')
    month = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Month')
    year = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Year')
    if month == '' and year == '' and root.find('./MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate') is not None:
        year = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate')
    vol = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/Volume')
    issue = xmlfind(root, './MedlineCitation/Article/Journal/JournalIssue/Issue')
    pages = xmlfind(root, './MedlineCitation/Article/Pagination/MedlinePgn')
    citation = journal + '. ' + (month + ' ' if len(month)>0 else '') + year + ';'
    issue = '(' + issue + ')' if len(issue)>0 else ''
    citation = citation + vol + issue + ':' if len(vol)>0 else ''
    citation = citation + pages + '.'
    paper['citation'] = citation

    y2 = xmlfind(root, './MedlineCitation/Article/ArticleDate/Year')
    if len(y2) > 0:
        year = y2
    if month in Mon:
        month = Mon[month]
    m2 = xmlfind(root, './MedlineCitation/Article/ArticleDate/Month')
    if len(m2) > 0:
        month = m2
    elif len(month) == 0 and len(year) == 4:
        month = '12'
    d2 = xmlfind(root, './MedlineCitation/Article/ArticleDate/Day')
    if len(d2) > 0:
        day = d2
    elif len(day) == 0 and len(year) == 4:
        day = '01'

    pubdate = year + '-' + month + '-' + day
    if len(year) != 4:
        pubdate = year
    paper['pubdate'] = pubdate

    if root.find('./PubmedData/History') is not None:
        for entry in root.find('./PubmedData/History'):
            if xmlfind(entry, '.', 'PubStatus') == 'received':
                pubdate = xmlfind(entry, './Year') + '-' + xmlfind(entry, './Month') + '-' + xmlfind(entry, './Day')
                paper['received'] = pubdate

    #paper['substances'] = []
    substances = dict()
    if root.find('./MedlineCitation/ChemicalList') is not None:
        for chemical in root.find('./MedlineCitation/ChemicalList'):
            if xmlfind(chemical, './RegistryNumber') != '0':
                substances[xmlfind(chemical, './NameOfSubstance', 'UI')] = [xmlfind(chemical, './NameOfSubstance', 'UI'), xmlfind(chemical, './RegistryNumber'), xmlfind(chemical, './NameOfSubstance')]
                #paper['substances'].append([xmlfind(chemical, './NameOfSubstance', 'UI'), xmlfind(chemical, './RegistryNumber'), xmlfind(chemical, './NameOfSubstance')])

    paper['cohort'] = 'N/A'
    paper['condition'] = 'N/A'
    paper['target'] = 'N/A'
    paper['mode'] = 'N/A'
    qualifiers = dict()
    if root.find('./MedlineCitation/MeshHeadingList') is not None:
        for meshheading in root.find('./MedlineCitation/MeshHeadingList'):
            ui = xmlfind(meshheading, 'DescriptorName', 'UI')
            if ui in substances.keys():
                qualifiers[ui] = []
                if meshheading.find('./QualifierName') is not None:
                    for qual in meshheading.findall('./QualifierName'):
                       #paper['substances'][i].append(qual.text)
                       qualifiers[ui].append(qual.text)
            elif ui == 'D000818':
                paper['cohort'] = 'Animals'
            elif ui == 'D006801':
                paper['cohort'] = 'Humans'
            elif meshheading.find('./QualifierName') is not None:
                for qual in meshheading.findall('./QualifierName'):
                    if qual.text in ['drug therapy']:
                        condition = xmlfind(meshheading, './DescriptorName')
                        for q2 in meshheading.findall('./QualifierName'):
                            if q2 != qual:
                                condition = condition + '/' + q2.text
                        condition = ui + ' ' + condition
                        if paper['condition'] != 'N/A':
                            paper['condition'] = paper['condition'] + '|' + condition
                        paper['condition'] = condition
                    elif qual.text in ['agonists', 'antagonists & inhibitors']:
                        paper['target'] = ui + " " + xmlfind(meshheading, './DescriptorName')
                        paper['mode'] = qual.text
    
    #print(qualifiers)
    #print(substances)
    delsub = []
    for ui in substances.keys():
        if ui in _mesh_sup and ui[0] == 'C':
            for hm in _mesh_sup[ui]['HM']:
                hm = hm.split('/*')
                if hm[0][0] == '*':
                    hm[0] = hm[0][1:]
                for sub in substances.keys():
                    if substances[sub][2] == hm[0]:
                        if ui not in qualifiers:
                            qualifiers[ui] = []
                        if len(hm) > 1 and hm[1] in qualifiers[sub]:
                            qualifiers[sub].remove(hm[1])
                        for qual in qualifiers[sub]:
                            if qual not in qualifiers[ui]:
                                qualifiers[ui].append(qual)
                        if sub not in delsub:
                            delsub.append(sub)
    for entry in delsub:
        del substances[entry]
    for entry in qualifiers.keys():
        if entry in substances.keys():
            substances[entry].append(qualifiers[entry])
    paper['substances'] = []
    for entry in substances.keys():
        paper['substances'].append(substances[entry])

    return paper

def grabPubMedTrials():
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(Serelaxin+AND+(clinical+trial[Publication+Type])+NOT+(review[Publication+Type]))&usehistory=y
    # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&WebEnv=MCID_603e8b64f287c4281515d660&query_key=1
    esearch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed"
    efetch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed"
    #ctquery = "(Serelaxin+AND+(clinical+trial[Publication+Type])+NOT+(review[Publication+Type]))"
    ctquery = "(clinical+trial[Publication+Type])+NOT+(review[Publication+Type])"
    response = requests.get(esearch+"&term="+ctquery+"&usehistory=y")
    root = xml.etree.ElementTree.fromstring(response.content)
    count = int(xmlfind(root, './Count'))
    print(count)
    query_key = xmlfind(root, './QueryKey')
    webenv = xmlfind(root, './WebEnv')
    retstart = 0
    while retstart < count:
        try:
            if not os.path.exists("pubmed/pmds"+str(retstart/1000)+".xml.gz"):
                r2 = requests.get(efetch+"&retmode=xml&WebEnv="+webenv+"&query_key="+query_key+"&retmax=1000&retstart="+str(retstart))
                fp = gzip.open("pubmed/pmds"+str(retstart/1000)+".xml.gz", "wb")
                fp.write(r2.content)
                fp.close()
            retstart = retstart + 1000
        except:
            print("oops! Trying again ...")
            response = requests.get(esearch+"&term="+ctquery+"&usehistory=y")
            root = xml.etree.ElementTree.fromstring(response.content)
            count = int(xmlfind(root, './Count'))
            print(count)
            query_key = xmlfind(root, './QueryKey')
            webenv = xmlfind(root, './WebEnv')

        print(retstart)

def parsePubMedTrials():
    prepMeSH()
    
    papers = dict()

    for i in range(0,878):
        tree = xml.etree.ElementTree.parse(gzip.open("pubmed/pmds"+str(i)+".0.xml.gz", 'rb'))
        root = tree.getroot()

        for entry in root:
            paper = parsePaper(entry)
            print(paper['PMID'])
            papers[paper['PMID']] = paper
            #print(paper)

    with gzip.open('papers.json.gz', 'wt') as f:
        json.dump(papers, f, ensure_ascii=False, indent=4)

def testPubMedTrials(query = None):
    prepMeSH()

    if query == None:
        tree = xml.etree.ElementTree.parse("efetch.xml")
        root = tree.getroot()
    else:
        esearch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed"
        efetch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed"
        #ctquery = "(Serelaxin+AND+(clinical+trial[Publication+Type])+NOT+(review[Publication+Type]))"
        ctquery = "("+query+"+AND+(clinical+trial[Publication+Type])+NOT+(review[Publication+Type]))"
        response = requests.get(esearch+"&term="+ctquery+"&usehistory=y")
        root = xml.etree.ElementTree.fromstring(response.content)
        count = int(xmlfind(root, './Count'))
        print(count)
        query_key = xmlfind(root, './QueryKey')
        webenv = xmlfind(root, './WebEnv')
        r2 = requests.get(efetch+"&retmode=xml&WebEnv="+webenv+"&query_key="+query_key+"&retmax=100")
        root = xml.etree.ElementTree.fromstring(r2.content)

    papers = dict()
    for entry in root:
        paper = parsePaper(entry)
        print(paper['PMID'])
        papers[paper['PMID']] = paper
        print(paper)
    
    return(papers)


def prepMeSH():

    # ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/
    # https://www.nlm.nih.gov/mesh/xmlconvert_ascii.html
    # https://www.nlm.nih.gov/mesh/dtype.html
    # https://www.nlm.nih.gov/mesh/xml_data_elements.html
    # https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/MSH/stats.html
    if len(_mesh_sup.keys()) > 0:
        return
    
    mitems = ['NM', 'RN', 'SO', 'N1', 'NO', 'UI']
    mmaps = dict()
    mmaps['MH'] = 'NM'
    mmaps['PRINT ENTRY'] = 'SY'
    mmaps['ENTRY'] = 'SY'
    mmaps['MS'] = 'SO'
    mlists = ['HM', 'RR', 'SY']
    files = ['c2021.bin', 'd2021.bin']
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

    return

def prepFromDir():

    with gzip.open('ctgovuniis.json.gz', 'rt') as f: # nctNumber, unii, displayTerm
        ctgovuniis = json.load(f)
    print('Loaded ctgovuniis from workbook')

    zf = zipfile.ZipFile('UNII_Data.zip') # https://fdasis.nlm.nih.gov/srs/jsp/srs/uniiListDownload.jsp
    files = zf.namelist()
    fp = zf.open(files[0])
    unii2pt = dict()
    unii2smi = dict()
    cas2unii = dict()
    for line in io.TextIOWrapper(fp, 'ascii'): # UNII	PT	RN	EC	...
        sline = line.split('\t')
        unii2pt[sline[0]] = sline[1]
        if len(sline) > 15 and len(sline[15]) > 0:
            unii2smi[sline[0]] = sline[15]
        cas2unii[sline[2]] = sline[0]
    fp.close()
    zf.close()
    print('Loaded UNII_Data')

    with gzip.open('papers.json.gz', 'rt') as f: # papers[PMID] = dict of PMID, DOI, title, citation, nct, phase, pubdate, received, [substances]
            # substances = [MeSH UI, UNII/CAS, name, role]    for roles see https://www.nlm.nih.gov/mesh/qualifiers_scopenotes.html
        papers = json.load(f)
    print('Loaded clinical trials from pubmed')

    with gzip.open('ctdata.json.gz', 'rt') as f: # ctdata[NCT] = [NCT, URL, condition, meshcondition, phase, status, type, start, end, title, interventions]
        ctdata = json.load(f)
    print('Loaded clinicaltrials.gov data')

    return unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis

def writeDrugPapers():

    prepMeSH()

    unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis = prepFromDir()

    da = []
    for entry in ctgovuniis:
        da.append(entry[1])
    da.sort()

    ignoreuniis = []
    #ignoreuniis.append(da[0])
    #for item in da:
    #    if ignoreuniis[-1] != item:
    #        ignoreuniis.append(item)

    substs = dict()
    counter = 0
    for pmid in papers.keys():
        paper = papers[pmid]
        for subst in paper['substances']:
            #if subst[1].rfind('-') > subst[1].find('-') and len(subst) > 3 and "therapeutic use" in subst[3] and "analogs & derivatives" not in subst[3]: # is a cas, not unii
            if len(subst) > 3 and ("therapeutic use" in subst[3] or "administration & dosage" in subst[3]) and "analogs & derivatives" not in subst[3]: # is a cas, not unii
                unii = subst[1]
                if subst[1] in cas2unii.keys():
                    unii = cas2unii[subst[1]]
                if unii not in ignoreuniis: # and unii in unii2smi.keys():
                    print(unii)
                    #print(subst)
                    #print(paper)
                    if unii not in substs:
                        substs[unii] = []
                    substs[unii].append(pmid)
                    counter = counter + 1

    drugPapers = dict()
    drugs_nounii = dict()
    substsct = []
    for key in substs.keys():
        substsct.append([len(substs[key]), key])
    substsct.sort()
    for entry in substsct:
        print(entry)
        drug = dict()
        drug['count'] = entry[0]
        drug['papers'] = []
        for pmid in substs[entry[1]]:
            #print(papers[pmid])
            drug['papers'].append(papers[pmid])
        if entry[1].rfind('-') > entry[1].find('-'): # CAS not mapped to a UNII
            for sub in papers[substs[entry[1]][0]]['substances']:
                if sub[1] == entry[1]:
                    if sub[0] in _mesh_sup:
                        drug['mesh'] = _mesh_sup[sub[0]]
                    else:
                        drug['mesh'] = [sub[0], sub[1]]
                    break
            drugs_nounii[entry[1]] = drug
        else:
            drugPapers[entry[1]] = drug
    with gzip.open('drugPapers.json.gz', 'wt') as f:
        json.dump(drugPapers, f, ensure_ascii=False, indent=4)
    with gzip.open('drugPapers_nounii.json.gz', 'wt') as f:
        json.dump(drugs_nounii, f, ensure_ascii=False, indent=4)

    return

def readDrugPapers():

    # "1TBM83QR9S": {
    #     "count": 1,
    #     "papers": [
    #         {
    #             "PMID": "30883047",
    #             "DOI": "10.1056/NEJMoa1901778",
    #             "title": "Antibody-Based Ticagrelor Reversal Agent in Healthy Volunteers.",
    #             "phase": "Phase 1",
    #             "nct": "NCT03492385",
    #             "citation": "N Engl J Med. 05 2019;380(19):1825-1833.",
    #             "pubdate": "2019-03-17",
    #             "cohort": "Humans",
    #             "condition": "N/A",
    #             "target": "N/A",
    #             "mode": "N/A",
    #             "substances": [
    #                 [
    #                     "C000622443",
    #                     "1TBM83QR9S",
    #                     "PB-2452",
    #                     [
    #                         "adverse effects",
    #                         "therapeutic use"
    #                     ]
    #                 ]
    #             ]
    #         }
    #     ]
    # },

    with gzip.open('drugPapers.json.gz', 'rt') as f:
        drugPapers = json.load(f)
    print('Loaded drug papers file')

    return(drugPapers)

def dated(datestamp):
    if datestamp == '':
        return datestamp
    sd = datestamp.split()
    y = sd[-1]
    if len(sd) > 1:
        m = Mon[sd[0]]
    else:
        m = '12'
    if len(sd) > 2:
        d = sd[1][:-1]
        if len(d) < 2:
            d = '0' + d
    else:
        d = '01'
    return y + '-' + m + '-' + d

def meshMetabolites():

    prepMeSH()

    metabNoUNII = []
    metabUNII = []
    for ui in _mesh_sup.keys():
        entry = _mesh_sup[ui]
        if 'NO' in entry.keys() and entry['NO'].lower().find('metabolite') > -1:
            unii = ''
            if len(entry['RN']) == 10 and entry['RN'].find('-') == -1 and entry['RN'].find(' ') == -1:
                unii = entry['RN']
            elif entry['RN'] in cas2unii.keys():
                unii = cas2unii[entry['RN']]
            elif 'RR' in entry.keys():
                for item in entry['RR']:
                    if len(item) == 10 and item.find('-') == -1 and item.find(' ') == -1:
                        unii = item
            if unii == '':
                metabNoUNII.append(entry)
            else:
                entry['UNII'] = unii
                metabUNII.append(entry)


    with gzip.open('MeSH-metabolites-nounii.json.gz', 'wt') as f:
        json.dump(metabNoUNII, f, indent=4)
    with gzip.open('MeSH-metabolites-unii.json.gz', 'wt') as f:
        json.dump(metabUNII, f, indent=4)    

    return

if __name__=="__main__":
    # TODO Biomarkers from clinical trials (blood, urine, csf); MeSH heading D054316 doesn't otherwise tag the marker unfortunately

    #papers = testPubMedTrials('32511251[pmid]')
    #papers = testPubMedTrials('Ro+24-7429')
    #sys.exit()

    readGSRSworkbook()

    indexCTzip()

    grabPubMedTrials()

    parsePubMedTrials()

    writeDrugPapers()

    unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis = prepFromDir()

    meshMetabolites()
    
    drugPapers = readDrugPapers()

    #with open('/Users/southalln/git/stitcher-rawinputs/files/rancho-export_2020-07-24_14-33.json', 'r') as f:
    #    ranchoDrugs = json.load(f)
    #print("Loaded Rancho drugs file")

    drugTrials = dict()
    for item in ctgovuniis: # nctNumber, unii, displayTerm
        if item[0] in ctdata.keys():
            if item[1] not in drugTrials.keys():
                drugTrials[item[1]] = []
            entry = ctdata[item[0]] # ctdata[NCT] = [NCT, URL, condition, meshcondition, phase, status, type, start, end, title, interventions]
            # [UNII, PHASE, START, END, NCT_ID, URL, TYPE, STATUS, CONDITION, TITLE]
            trial = [item[1], entry[4], dated(entry[7]), dated(entry[8]), entry[0], entry[1], entry [6], entry[5], entry[2], entry[9]]
            drugTrials[item[1]].append(trial)

    if 'MLW2GKK8LI' in drugTrials.keys():
        print(drugTrials['MLW2GKK8LI'])

    count = 0
    for unii in drugPapers.keys():
        dp = drugPapers[unii]
        highestphase = 'Not Applicable'
        for paper in dp['papers']:
            if paper['phase'] > highestphase:
                highestphase = paper['phase']
        if unii[0:3] != 'EC ' and (dp['count'] > 1 or highestphase > 'Not Applicable') and unii not in drugTrials.keys():
            drugTrials[unii] = []
            for paper in dp['papers']:
                # [UNII, PHASE, START, END, NCT_ID, URL, TYPE, STATUS, CONDITION, TITLE]
                date = ''
                if 'pubdate' in paper.keys():
                    date = paper['pubdate']
                if 'received' in paper.keys():
                    date = paper['received']
                nct = ''
                url = ''
                if 'nct' in paper.keys():
                    nct = paper['nct']
                    url = 'https://clinicaltrials.gov/ct2/show/' + nct
                elif 'citation' in paper.keys():
                    nct = paper['citation']
                    if 'DOI' in paper.keys() and len(paper['DOI']) > 0:
                        url = 'https://dx.doi.org/' + paper['DOI']
                    else:
                        url = 'https://pubmed.ncbi.nlm.nih.gov/' + paper['PMID']
                ttype = ''
                if paper['cohort'] == 'Humans':
                    ttype = 'Human clinical trial'
                elif paper['cohort'] == 'Animals':
                    ttype = 'Veterinary clinical trial'
                status = 'Completed'
                condition = paper['condition']
                if condition.find(' ') > -1:
                    condition = condition[condition.find(' '):]
                trial = [unii, paper['phase'], '', date, nct, url, ttype, status, condition, paper['title']]
                drugTrials[unii].append(trial)
            if unii == 'O4601UER1Z':
                print(unii)
                print(dp)
            count = count + 1
    print(count)

    with gzip.open('NCT_REPORT.txt.gz', 'wt') as f:
        f.write('UNII\tPHASE\tSTART\tEND\tNCT_ID\tURL\tTYPE\tSTATUS\tCONDITION\tTITLE\n')
        for unii in drugTrials.keys():
            trials = drugTrials[unii]
            trials.sort()
            trials.reverse()
            idx = 0
            while idx<len(trials) and (idx<3 or trials[idx][1] > 'Phase 2'):
                idx = idx + 1
            for trial in trials[:idx]:
                f.write('\t'.join(trial) + '\n')
        f.close()


    

