#!/usr/bin/python3
from openpyxl import load_workbook
import zipfile
import xml
import gzip
import json

from ct_utils import xmlfind


def parseCT(zf, ctfile):
    ctnum = ctfile[12:-4]
    url = 'https://clinicaltrials.gov/ct2/show/' + ctnum
    try:
        fp = zf.open(ctfile)
    except:
        return([ctnum])
    tree = xml.etree.ElementTree.parse(fp)
    root = tree.getroot()

    title = xmlfind(root, './brief_title')
    phase = xmlfind(root, './phase')
    if phase == 'N/A':
        phase = 'Not Applicable'
    study_type = xmlfind(root, './study_type')

    status = xmlfind(root, './overall_status')
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

def indexCTzip():
    zf = zipfile.ZipFile('../stitcher-inputs/temp/AllPublicXML.zip') # https://classic.clinicaltrials.gov/AllPublicXML.zip
    files = zf.namelist()
    ctdata = dict()
    for filename in files[1:]: # ['Contents.txt', 'NCT0000xxxx/NCT00000102.xml', 'NCT0000xxxx/NCT00000104.xml', 'NCT0000xxxx/NCT00000105.xml', 'NCT0000xxxx/NCT00000106.xml', 'NCT0000xxxx/NCT00000107.xml', 'NCT0000xxxx/NCT00000108.xml' ...
        ctnum = filename[12:-4]
        print(ctnum)
        ctdata[ctnum], alias = parseCT(zf, filename)
        if alias != '':
            ctdata[alias] = ctdata[ctnum]
    zf.close()

    with gzip.open('../stitcher-inputs/temp/ctdata.json.gz', 'wt') as f:
        json.dump(ctdata, f, ensure_ascii=False, indent=4)

    return

def readGSRSworkbook(workbook_file_name):
    data = []
    wb = load_workbook(workbook_file_name, read_only=True)
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

    with gzip.open('../stitcher-inputs/temp/ctgovuniis.json.gz', 'wt') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    return(data[header+1:])


if __name__=="__main__":
    readGSRSworkbook(snakemake.input[0])
    indexCTzip()


    

