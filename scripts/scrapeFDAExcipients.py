import csv
import glob
import io
import os
import re
import time
import urllib.request
import zipfile


def getTimeStamp():
    ts = time.gmtime()
    return time.strftime("%Y-%m-%d", ts)


def getUNIIlist(maindir):
    uniifile = "../stitcher-inputs/temp/UNIIs.zip"
    zfp = zipfile.ZipFile(uniifile, 'r')
    names = zfp.namelist()
    fp = zfp.open(names[-1], 'r')
    line = fp.readline()
    if line[:-2] != "Name\tType\tUNII\tDisplay Name":
        raise ValueError('Problem reading UNII file:' + line)
    line = fp.readline()
    uniiPT = dict()
    uniiALL = dict()
    while line != "":
        sline = line[:-2].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading UNII fileline:' + line)
        uniiPT[sline[3]] = sline[2]
        if sline[0][-14:] == " [ORANGE BOOK]":
            sline[0] = sline[0][:-14]
        uniiALL[sline[0]] = sline[2]
        line = fp.readline()
    print("UNIIs in memory:", len(uniiPT), len(uniiALL))

    return uniiPT, uniiALL


if __name__ == "__main__":
    iig = dict()
    iig[''] = []
    iig['N/A'] = []

    uris = [
        {
            'base': 'https://www.fda.gov',
            'page': '/Drugs/InformationOnDrugs/ucm113978.htm'
        }
    ]

    for uri_index, uri_obj in enumerate(uris):
        print("HOME URL: " + uri_obj['base'] + uri_obj['page'])

        with urllib.request.urlopen(uri_obj['base'] + uri_obj['page']) as response:
            html = response.read()
            pattern = r'<a [^<>]*href=[\'\"]([^<>\'\"]+)[\'\"][^<>]*>([^<>]+(?:Inactive )?Ingredient?s? Database(?: Download)? File)</a>'
            matches = re.findall(pattern, html.decode('utf-8'))

            for link_index, link in enumerate(matches):
                if link[0].startswith('http'):
                    dbURL = link[0]
                else:
                    dbURL = uri_obj['base'] + link[0]
                print(f"{uri_index}-{link_index}-{link[1]}")
                iigfile = f"../stitcher-inputs/temp/current-iig-{uri_index}-{link_index}.zip"
                print(f"\n\n{dbURL}\n\n")
                syscall = "curl -sL --insecure -o " + iigfile + " " + dbURL
                if not os.path.exists(iigfile):
                    print(syscall)
                    os.system(syscall)

    # the historical files seems like they never made it into FDAexcipients, it was just there for investigation
    # matching_files = glob.glob("../stitcher-inputs/temp/current-iig-*.zip")
    # matching_files = glob.glob("../stitcher-inputs/temp/current-iig-0-20.zip")  # equivalent to the previous version probably
    matching_files = glob.glob("../stitcher-inputs/temp/current-iig-0-0.zip")  # so we'll just use the current one
    for file in matching_files:
        with zipfile.ZipFile(file, 'r') as zfp:
            names = zfp.namelist()
            data_file = [filename for filename in names if (
                        filename.endswith('.csv') or filename.endswith('txt')) and 'change_log' not in filename.lower()]
            if not data_file or len(data_file) > 1:
                raise Exception(f"can't find data file in {file}: {names}")
            with zfp.open(data_file[0], 'r') as fp:
                print(f"{file}: {data_file[0]}")
                content = fp.read().decode('utf-8')
                content_io = io.StringIO(content)

                if content.count('~') > 1000:
                    delimiter = "~"
                elif content.count('\t') > 1000:
                    delimiter = "\t"
                else:
                    delimiter = ","
                if content.count('TOPICAL;  LOTION') > 10:
                    combined_route_and_dosage_form = True
                else:
                    combined_route_and_dosage_form = False

                has_header_row = content.upper().find('INGREDIENT_NAME') >= 0
                reader = csv.reader(content_io, delimiter=delimiter)

                if has_header_row:
                    _ = next(reader)

                line = next(reader)
                try:
                    while len(line) > 0:
                        if combined_route_and_dosage_form:
                            INGREDIENT_NAME, ROUTE_AND_DOSAGE_FORM, CAS_NUMBER, UNII, POTENCY_AMOUNT, POTENCYUNIT, *whatever_else = line
                            ROUTE, DOSAGE_FORM = ROUTE_AND_DOSAGE_FORM.split(';')
                        else:
                            INGREDIENT_NAME, ROUTE, DOSAGE_FORM, CAS_NUMBER, UNII, POTENCY_AMOUNT, POTENCYUNIT, *whatever_else = line
                        if UNII not in iig:
                            iig[UNII] = []
                        iig[UNII].append(
                            (INGREDIENT_NAME, ROUTE, DOSAGE_FORM, CAS_NUMBER, UNII, POTENCY_AMOUNT, POTENCYUNIT))
                        line = next(reader)
                except StopIteration:
                    pass

    #
    # # check old iig files
    # keys = iigfiles.keys()
    # del keys[keys.index(getTimeStamp())]
    # keys.sort()
    # keys.reverse()
    # for key in keys:
    #     if key != getTimeStamp() and os.path.isfile(iigfiles[key]):
    #         #print key
    #         zfp = zipfile.ZipFile(iigfiles[key], 'r')
    #         names = zfp.namelist()
    #         csvfile = ""
    #         sep = ','
    #         shead = header.split(",")
    #         items = len(shead)
    #         for item in names:
    #             if item.find(".csv") > -1:
    #                 csvfile = item
    #             elif item.find("iig-Internet.txt") > -1:
    #                 csvfile = item
    #                 sep = '\t'
    #             elif item.find(".txt") > -1:
    #                 csvfile = item
    #                 sep = '~'
    #         if csvfile == "":
    #             continue
    #         fp = zfp.open(csvfile, 'r')
    #         line = fp.readline()
    #         line = line.replace(sep, ",")
    #         if sep != '~' and line[:min(len(header),len(line)-2)].upper() != header[:min(len(header),len(line)-2)].upper():
    #             raise ValueError('Problem reading IIG file:\n'+line+"\n"+header)
    #
    #         line = fp.readline()
    #         count = 0
    #         while line != "":
    #             sline = readDelLine(line, sep)
    #             if sep == '~':
    #                 sline.insert(2, (sline[1].split(";  "))[-1])
    #                 sline.insert(2, (sline[1].split(";  "))[0])
    #                 del sline[1]
    #             if len(sline) < items:
    #                 raise ValueError('Wrong number of items in iig entry: '+str(len(sline))+":"+str(len(shead)))
    #                 items = min(len(sline), len(shead))
    #             entry = dict()
    #             for i in range(items):
    #                 entry[shead[i]] = sline[i]
    #             rec = sline[shead.index('UNII')]
    #             if not iig.has_key(rec):
    #                 print "Missing unii entry:", key, rec, entry
    #                 iig[rec] = [] # don't actually save/use this info yet - requires review
    #             elif rec == '1':
    #                 found = False
    #                 uniq = entry['UNII']# + ":" + entry['DOSAGE_FORM']
    #                 for item in iig[rec]:
    #                     uniq2 = item['UNII']# + ":" + item['DOSAGE_FORM']
    #                     if uniq2 == uniq:
    #                         found = True
    #                 if not found:
    #                     print "not found"
    #                     print key
    #                     print entry
    #                     sys.exit()
    #             line = fp.readline()
    #             count = count + 1
    #         fp.close()
    #         zfp.close()
    #         #print count
    #
    # generate output file
    '''
    public EventKind kind;
    public String source;
    public Object id;
    public Date startDate;
    public Date endDate;
    public String active;
    public String jurisdiction;
    public String comment; // reference
    public String route;
    public String approvalAppId;
    public String product;
    public String sponsor;
    public String URL;
    '''
    # header = "INGREDIENT_NAME,ROUTE,DOSAGE_FORM,CAS_NUMBER,UNII,POTENCY_AMOUNT,POTENCYUNIT"
    outfile = "../stitcher-inputs/active/FDAexcipients.txt"
    fp = open(outfile, 'w')
    header = "UNII\tIIG-ID\tRoute\tStrength\tIIG-URL\n"
    fp.write(header)
    for unii_key in iig.keys():
        for entry in iig[unii_key]:
            INGREDIENT_NAME, ROUTE, DOSAGE_FORM, CAS_NUMBER, _, POTENCY_AMOUNT, POTENCYUNIT = entry
            if not INGREDIENT_NAME or len(INGREDIENT_NAME) == 0:
                continue
            id = f"{INGREDIENT_NAME}:{ROUTE}:{DOSAGE_FORM}:{POTENCYUNIT}"
            strength = f"{POTENCY_AMOUNT}{POTENCYUNIT}"
            if strength == 'NA':
                strength = ''
            elif len(strength) > 0 and len(ROUTE) > 0:
                strength = f"{strength} {ROUTE}"
            if len(strength) > 0 and len(DOSAGE_FORM) > 0:
                strength = f"{strength} {DOSAGE_FORM}"

            url = "https://www.accessdata.fda.gov/scripts/cder/iig/index.cfm"
            ing = INGREDIENT_NAME
            if ing[0].isalpha() or ing[0].isdigit():
                url = url + "?event=browseByLetter.page&Letter=" + ing[0]
            elif ing[1].isalpha():
                url = url + "?event=browseByLetter.page&Letter=" + ing[1]
            outline = unii_key + "\t" + id + "\t" + ROUTE + "\t" + strength + "\t" + url + "\n"
            fp.write(outline)
    fp.close()
