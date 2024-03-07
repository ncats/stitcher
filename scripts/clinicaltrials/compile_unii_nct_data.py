#!/usr/bin/python3
import zipfile
import gzip
import json
import io

from ct_utils import prepMeSH, month_dictionary

_mesh_sup = prepMeSH()


def prepFromDir():
    with gzip.open('../stitcher-inputs/temp/ctgovuniis.json.gz', 'rt') as f:  # nctNumber, unii, displayTerm
        ctgovuniis = json.load(f)
    print('Loaded ctgovuniis from workbook')

    # zf = zipfile.ZipFile('UNII_Data.zip')  # https://fdasis.nlm.nih.gov/srs/jsp/srs/uniiListDownload.jsp
    zf = zipfile.ZipFile('../stitcher-inputs/temp/UNII_data.zip')  # https://fdasis.nlm.nih.gov/srs/jsp/srs/uniiListDownload.jsp
    files = zf.namelist()
    print(f"Loading uniis from UNII_data.zip/{files[2]}")
    fp = zf.open(files[2])
    unii2pt = dict()
    unii2smi = dict()
    cas2unii = dict()
    for line in io.TextIOWrapper(fp, 'ascii'):  # UNII	PT	RN	EC	...
        sline = line.split('\t')
        unii2pt[sline[0]] = sline[1]
        if len(sline) > 15 and len(sline[15]) > 0:
            unii2smi[sline[0]] = sline[15]
        cas2unii[sline[2]] = sline[0]
    fp.close()
    zf.close()
    print('Loaded UNII_Data')

    with gzip.open('../stitcher-inputs/temp/papers.json.gz',
                   'rt') as f:  # papers[PMID] = dict of PMID, DOI, title, citation, nct, phase, pubdate, received, [substances]
        # substances = [MeSH UI, UNII/CAS, name, role]    for roles see https://www.nlm.nih.gov/mesh/qualifiers_scopenotes.html
        papers = json.load(f)
    print('Loaded clinical trials from pubmed')

    with gzip.open('../stitcher-inputs/temp/ctdata.json.gz',
                   'rt') as f:  # ctdata[NCT] = [NCT, URL, condition, meshcondition, phase, status, type, start, end, title, interventions]
        ctdata = json.load(f)
    print('Loaded clinicaltrials.gov data')

    return unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis


def writeDrugPapers():
    unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis = prepFromDir()

    da = []
    for entry in ctgovuniis:
        da.append(entry[1])
    da.sort()

    ignoreuniis = []

    substs = dict()
    counter = 0
    for pmid in papers.keys():
        paper = papers[pmid]
        for subst in paper['substances']:
            # if subst[1].rfind('-') > subst[1].find('-') and len(subst) > 3 and "therapeutic use" in subst[3] and "analogs & derivatives" not in subst[3]: # is a cas, not unii
            if len(subst) > 3 and ("therapeutic use" in subst[3] or "administration & dosage" in subst[
                3]) and "analogs & derivatives" not in subst[3]:  # is a cas, not unii
                unii = subst[1]
                if subst[1] in cas2unii.keys():
                    unii = cas2unii[subst[1]]
                if unii not in ignoreuniis:  # and unii in unii2smi.keys():
                    # print(unii)
                    # print(subst)
                    # print(paper)
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
        # print(entry)
        drug = dict()
        drug['count'] = entry[0]
        drug['papers'] = []
        for pmid in substs[entry[1]]:
            # print(papers[pmid])
            drug['papers'].append(papers[pmid])
        if entry[1].rfind('-') > entry[1].find('-'):  # CAS not mapped to a UNII
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
    with gzip.open('../stitcher-inputs/temp/drugPapers.json.gz', 'wt') as f:
        json.dump(drugPapers, f, ensure_ascii=False, indent=4)
    with gzip.open('../stitcher-inputs/temp/drugPapers_nounii.json.gz', 'wt') as f:
        json.dump(drugs_nounii, f, ensure_ascii=False, indent=4)

    return


def readDrugPapers():
    with gzip.open('../stitcher-inputs/temp/drugPapers.json.gz', 'rt') as f:
        drugPapers = json.load(f)
    print('Loaded drug papers file')

    return (drugPapers)

Mon = month_dictionary()
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

    with gzip.open('../stitcher-inputs/temp/MeSH-metabolites-nounii.json.gz', 'wt') as f:
        json.dump(metabNoUNII, f, indent=4)
    with gzip.open('../stitcher-inputs/temp/MeSH-metabolites-unii.json.gz', 'wt') as f:
        json.dump(metabUNII, f, indent=4)

    return


if __name__ == "__main__":
    print('starting')
    writeDrugPapers()

    unii2pt, unii2smi, cas2unii, ctdata, papers, ctgovuniis = prepFromDir()

    meshMetabolites()

    drugPapers = readDrugPapers()

    drugTrials = dict()
    for item in ctgovuniis:  # nctNumber, unii, displayTerm
        if item[0] in ctdata.keys():
            if item[1] not in drugTrials.keys():
                drugTrials[item[1]] = []
            entry = ctdata[item[
                0]]  # ctdata[NCT] = [NCT, URL, condition, meshcondition, phase, status, type, start, end, title, interventions]
            # [UNII, PHASE, START, END, NCT_ID, URL, TYPE, STATUS, CONDITION, TITLE]
            trial = [item[1], entry[4], dated(entry[7]), dated(entry[8]), entry[0], entry[1], entry[6], entry[5],
                     entry[2], entry[9]]
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
        if unii[0:3] != 'EC ' and (
                dp['count'] > 1 or highestphase > 'Not Applicable') and unii not in drugTrials.keys():
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

    with gzip.open('../stitcher-inputs/active/NCT_REPORT.txt.gz', 'wt') as f:
        f.write('UNII\tPHASE\tSTART\tEND\tNCT_ID\tURL\tTYPE\tSTATUS\tCONDITION\tTITLE\n')
        for unii in drugTrials.keys():
            trials = drugTrials[unii]
            trials.sort()
            trials.reverse()
            idx = 0
            while idx < len(trials) and (idx < 3 or trials[idx][1] > 'Phase 2'):
                idx = idx + 1
            for trial in trials[:idx]:
                f.write('\t'.join(trial) + '\n')
        f.close()
