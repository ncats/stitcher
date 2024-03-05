from ct_utils import xmlfind, prepMeSH, month_dictionary
import xml.etree.ElementTree as ET
import gzip
import glob
import json

_mesh_sup = prepMeSH()
print(len(_mesh_sup))

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
    Mon = month_dictionary()
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
                substances[xmlfind(chemical, './NameOfSubstance', 'UI')] = \
                    [
                        xmlfind(chemical, './NameOfSubstance', 'UI'),
                        xmlfind(chemical, './RegistryNumber'),
                        xmlfind(chemical, './NameOfSubstance')
                    ]
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

def parsePubMedTrials():
    papers = dict()

    matching_files = glob.glob("../stitcher-inputs/temp/pubmed/*.xml.gz")

    for file in matching_files:
        print(f"parsing: {file}")
        tree = ET.parse(gzip.open(file, 'rb'))
        root = tree.getroot()

        for entry in root:
            if not entry:
                continue
            paper = parsePaper(entry)
            # print(paper['PMID'])
            papers[paper['PMID']] = paper
            #print(paper)

    with gzip.open('../stitcher-inputs/temp/papers.json.gz', 'wt') as f:
        json.dump(papers, f, ensure_ascii=False, indent=4)


if __name__=="__main__":

    parsePubMedTrials()
