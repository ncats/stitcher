import os
import sys
import time
import json
import string
import urllib2
import zipfile
import gzip
import numpy
#import matplotlib.pyplot as plt
#from scipy.interpolate import interp1d

resolverCache = dict()
resolverCache["PEMETREXED SODIUM"] = "2PKU919BA9"
resolverCache["NETARSUDIL DIMESYLATE"] = "VL756B1K0U"
resolverCache["INSULIN LISPRO RECOMBINANT"] = "GFX7QIS1II"
resolverCache["INSULIN SUSP ISOPHANE RECOMBINANT HUMAN"] = "1Y17CTI5SR"
resolverCache["PITAVASTATIN MAGNESIUM"] = "M5681Q5F9P"
resolverCache["IVACAFTOR, TEZACAFTOR"] = "8RW88Y506K"

def getTimeStamp():
    ts = time.gmtime()
    return time.strftime("%Y-%m-%d", ts)

def getOBZipURL():
    #zipurl = "https://www.fda.gov/downloads/Drugs/InformationOnDrugs/UCM163762.zip"
    zipurl = "https://www.fda.gov/media/76860/download"
    return zipurl

def getUNIIZipURL():
    zipurl = "https://fdasis.nlm.nih.gov/srs/download/srs/UNIIs.zip"
    return zipurl

def getDrugsFDAZipURL():
    #zipurl = "https://www.fda.gov/downloads/Drugs/InformationOnDrugs/UCM527389.zip"
    zipurl = "https://www.fda.gov/media/89850/download"
    #page = urllib2.urlopen('https://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm').read()
    #i = page.find('Drugs@FDA Download File')
    #j = page.rfind('href="/downloads/Drugs/InformationOnDrugs/', 0, i)
    #k = page.find('\"', j+6)
    #zipurl = "https://www.fda.gov/"+page[j+6:k]
    return zipurl

def getMainDir():
    curr = os.getcwd()
    if curr[-8:] == '/scripts':
        curr = curr[:-8]
    if curr[-5:] == '/temp':
        curr = curr[:-5]

    if not os.path.exists(curr+"/.git") or not os.path.exists(curr+"/data") or not os.path.exists(curr+"/scripts"):
        raise ValueError('Could not identify repo head from current directory')

    if not os.path.exists(curr+"/temp"):
        os.mkdir(curr+"/temp")

    return curr

def resolveName(name): # returns empty string if resolver can't do anything with this name
    if name in resolverCache.keys():
        return resolverCache[name]

    try:
        unii = resolveNameTripod(name)
        if len(unii) < 10:
            if name[-4:] == " KIT": # TECHNETIUM TC-99M TEBOROXIME KIT
                sname = name[:-4]
                unii = resolveNameTripod(sname)
            elif name[-5:-4] == "-": # PEGFILGRASTIM-JMDB
                sname = name[:-5]
                unii = resolveNameTripod(sname)
        resolverCache[name] = unii
        return unii
    except:
        raise
        return ''

def resolveNameTripod(name):
    unii = ''
    #response = urllib2.urlopen("https://tripod.nih.gov/servlet/resolverBeta3/unii", "structure="+name+"&format=json&apikey=5fd5bb2a05eb6195").read()
    #r = json.loads(response)
    #if len(r) > 0 and r[0].has_key('response'):
    #    resolverCache[name] = r[0]['response']
    #    if r[0]['input'] == unicode(name) and r[0]['source'] == unicode('FDA-SRS') and len(r[0]['response']) == 10:
    #        return r[0]['response']
    #else:

    request = "https://tripod.nih.gov/ginas/app/api/v1/substances/search?q=root_names_name:\"^"+urllib2.quote(name)+"$\""
    response = "{\"total\":0}"
    try:
        response = urllib2.urlopen(request).read()
    except:
        response
    r = json.loads(response)
    if int(r['total']) > 0:
        for i in range(0, int(r['total'])):
            if r['content'][i].has_key('_approvalIDDisplay'):
                if unii == '':
                    unii = r['content'][i]['_approvalIDDisplay']
                elif unii != r['content'][i]['_approvalIDDisplay']:
                    raise ValueError("conflicting UNIIs returned:"+name+":"+resolverCache[name]+":"+request)
            elif r['content'][i].has_key('approvalID'):
                if unii == '':
                    unii = r['content'][i]['approvalID']
                elif unii != r['content'][i]['approvalID']:
                    raise ValueError("conflicting UNIIs returned:"+name+":"+resolverCache[name]+":"+request)
            else:
                raise ValueError("UNIIs response not properly formatted:"+name)
    return unii

def parseIngred(ing, uniiPT, uniiALL, missing):
    ingreds = []
    ing = ing.strip().upper()
    if ing not in uniiPT:
        ing = ing + '; '
        while ing.find('; ') > -1:
            si = ing[:ing.find('; ')].strip()
            if si in uniiPT:
                ingreds.append(uniiPT[si])
            elif si in uniiALL:
                ingreds.append(uniiALL[si])
            elif si.find(';') > si.find('(') and si.find(';') < si.find(')') and si.find(')') == len(si)-1: #TRIPLE SULFA (SULFABENZAMIDE;SULFACETAMIDE;SULFATHIAZOLE)
                ing = ing[len(si):]
                si = si[si.find('(')+1:-1]
                si = si.replace(';', '; ')
                ing = '; ' + si + ing
                si = ''
            elif si.find(';') > -1: #ATAZANAVIR SULFATE;RITONAVIR;LAMIVUDINE;ZIDOVUDINE
                ing = ing[len(si):]
                si = si.replace(';', '; ')
                ing = '; ' + si + ing
                si = ''
            else:
                match = resolveName(si)
                if len(match) == 10:
                    ingreds.append(match)
                else:
                    if not missing.has_key(si):
                        sys.stderr.write("Ingredient does not map to UNII:"+si+"\t"+NDA+"\n")
                        missing[si] = []
                    missing[si].append(NDA)
            ing = ing[ing.find('; ')+2:]
        if len(ing) > 0 and ing not in uniiPT:
            raise ValueError('Problem parsing ingredients:'+ing)
    else:
        ingreds.append(uniiPT[ing])
    return ingreds

def readTabFile(filename, header = True, delim = '\t'):
    fp = open(filename, 'r')
    data = readTabFP(fp, header, delim)
    fp.close()
    return data

def readTabFP(fp, header = True, delim = '\t'):
    data = dict()
    line = fp.readline()
    eol = -1
    if line[-2] == '\r' or line[-2] == '\n':
        eol = -2
    elems = 0
    if header:
        sline = line[:eol].split(delim)
        data['header'] = sline
        elems = len(sline)
        line = fp.readline()
    data['table'] = []
    while line != "":
        sline = line[:eol].split(delim)
        if len(sline) < elems:
            elems = len(sline)
        data['table'].append(sline)
        line = fp.readline()

    while len(data['header']) < elems:
        data['header'].append('')
    for i in range(len(data['table'])):
        while len(data['table'][i]) < elems:
            data['table'][i].append('')

    return data

def apprDateRegression(prods):
    applimit = 200000
    window = 10000
    dataxs = dict()
    datays = dict()
    for prod in prods.keys():
        date = prods[prod][-2]
        if date != '':
            ts = time.mktime(time.strptime(date, "%Y-%m-%d"))
            source = prods[prod][-1]
            kind = prods[prod][-4]
            appl = int(prods[prod][0][0:6])
            if appl < applimit and (date != '1982-01-01' or source != 'OrangeBook'):
                kind1 = kind+str(2*int(float(appl)/window)+1)
                kind2 = kind+str(2*int(float(appl)/window+0.5))
                #if appl < 5000:
                #    print appl, kind1, kind2
                if not dataxs.has_key(kind1):
                    dataxs[kind1] = []
                    datays[kind1] = []
                if not dataxs.has_key(kind2):
                    dataxs[kind2] = []
                    datays[kind2] = []
                dataxs[kind1].append(appl)
                datays[kind1].append(ts)
                dataxs[kind2].append(appl)
                datays[kind2].append(ts)

    models = dict()
    for kind in dataxs.keys():
        #print kind, len(dataxs[kind])
        if len(dataxs[kind]) > 20:
            regr = numpy.poly1d(numpy.polyfit(dataxs[kind], datays[kind], 1))
            models[kind] = regr
    figs = ['NDA', 'BLA', 'ANDA']
    for figt in figs:
        fig, ax = plt.subplots()
        for kind in models.keys():
            if kind[0:len(figt)] == figt:
                range = int(kind[len(figt):])
                ax.scatter(dataxs[kind],datays[kind])
                #print range, range*window/2, (range+1)*window/2
                xp = numpy.linspace((range-1)*window/2+window/4, range*window/2+window/4, 100)
                yp = models[kind](xp)
                ax.plot(xp, yp)
        plt.ylim(-1500000000,2000000000)
        plt.xlim(-window,applimit+window)
        fig.tight_layout()
        plt.savefig(figt+'.png')

    for prod in prods.keys():
        date = prods[prod][-2]
        source = prods[prod][-1]
        kind = prods[prod][-4]
        appl = int(prods[prod][0][0:6])
        kind = kind+str(int(2.*float(appl)/window+0.5))
        if kind[0:3] == 'NDA' and (date == '' or (date == '1982-01-01' and source == 'OrangeBook')):
            if models.has_key(kind):
                pred = models[kind](appl)
                pred = time.strftime("%Y-%m-%d", time.gmtime(pred))
                if date == '1982-01-01' and source == 'OrangeBook':
                    if pred < '1982-01-01':
                        prods[prod][-2] = pred
                        prods[prod][-1] = 'PREDICTED'
                elif date == '':
                    prods[prod][-2] = pred
                    prods[prod][-1] = 'PREDICTED'
    return prods

def writeInitApp(outfp, unii, early, earlyDate):
    date = ''
    year = ''
    if len(earlyDate) > 0:
        date = earlyDate[5:7]+"/"+earlyDate[8:]+"/"+earlyDate[0:4]
        year = date[-4:]
    #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']
    method = early[-1]
    apptype = early[-4]
    appsponsor = early[-3]
    product = early[-8]
    appno = ''
    appurl = ''
    active = "true"
    if early[-5] != "Prescription" and early[-5] != "Over-the-counter":
        active = "false"

    if early[-1].find('https://www.accessdata.fda.gov/') > -1:
        appno = early[0][0:6]

        appurl = early[-1]
        method = "Drugs@FDA"
    elif len(early[0]) > 6 and early[0][6] == '/':
        appno = early[0][0:6]

        if method == "PREDICTED":
            year = "[Approval Date Uncertain] "+year

        appurl = "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo="+appno
    else:
        #outfp = fperr
        active = "false"
        if method.find('OB NME Appendix 1950-') > -1:
            appno = early[0][0:6]
        elif method.find('http') > -1:
            appurl = method
            method = "Literature"

        for otherunii in activeMoiety[unii]:
            if UNII2prods.has_key(otherunii):
                for prod in UNII2prods[otherunii]:
                    entry = prods[prod]
                    if entry[0] == early[0]:
                        unii = otherunii

    #comment = apptype+'|'+appno+'|'+appsponsor+'|'+product+'|'+appurl+'|'
    #comment = comment + early[0].decode('latin-1').encode('ascii', 'replace')
    #outline = unii+"\tApproval Year\t"+year+"\t\t"+comment+"\t"+date+"\t"+method+"\n"
    comment = early[0].decode('latin-1').encode('ascii', 'replace')
    outline = unii+"\t"+year+"\t"+date+"\t"+method+"\t"+apptype+"\t"+appno+"\t"+appsponsor+"\t"+product+"\t"+appurl+"\t"+active+"\t"+comment+"\n"
    outfp.write(outline)

if __name__=="__main__":

    maindir = getMainDir()

    drugsAtfdafile = maindir+"/temp/drugsAtfda-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+drugsAtfdafile + " " + getDrugsFDAZipURL()
    print syscall

    if not os.path.exists(drugsAtfdafile):
        os.system(syscall)

    uniifile = maindir+"/temp/UNIIs-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+uniifile + " " + getUNIIZipURL()
    print syscall

    if not os.path.exists(uniifile):
        os.system(syscall)
    obfile = maindir+"/temp/orangeBook-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+obfile + " " + getOBZipURL()
    print syscall
    if not os.path.exists(obfile):
        os.system(syscall)

    appYrsfile = maindir+"/scripts/data/approvalYears.txt"
    if not os.path.exists(appYrsfile):
        raise ValueError("Can't read PREDICTED approvals from prior file: "+appYrsfile)

    gsrsDumpfile = maindir+'/data/dump-public-2019-04-03.gsrs'
    if not os.path.exists(gsrsDumpfile):
        raise ValueError("Can't find GSRS dump file for active moiety lookup: "+gsrsDumpfile)

    fdaNMEfile = maindir+'/scripts/data/FDA-NMEs-2018-08-07.txt'
    if not os.path.exists(fdaNMEfile):
        raise ValueError("Can't find FDA NMEs file for historical approval dates: "+fdaNMEfile)

    zfp = zipfile.ZipFile(uniifile, 'r')
    names = zfp.namelist()
    fp = zfp.open(names[-1], 'r')
    line = fp.readline()

    if line[:-2] != "Name\tType\tUNII\tDisplay Name":
        raise ValueError('Problem reading UNII file:'+line)

    line = fp.readline()
    uniiPT = dict()
    uniiALL = dict()

    while line != "":
        sline = line[:-2].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading UNII fileline:'+line)
        uniiPT[sline[3]] = sline[2]
        if sline[0][-14:] == " [ORANGE BOOK]":
            sline[0] = sline[0][:-14]
        uniiALL[sline[0]] = sline[2]
        line = fp.readline()
    print "UNIIs in memory:", len(uniiPT), len(uniiALL)

    zfp = zipfile.ZipFile(drugsAtfdafile, 'r')
    fp = zfp.open('Products.txt', 'r')
    #ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard
    #0       1          2     3         4              5         6                 7
    line = fp.readline()
    if line[:-2] != "ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard":
        raise ValueError('Problem reading Products file:'+line)
    line = fp.readline()
    UNII2prods = dict()
    prods = dict() # prods[NDA/part no] = [NDA/part, prodName, form, strength, status, app type, sponsor, year, ref]
    #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']

    missing = dict()
    while line != "":
        sline = line[:-2].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading Products file2:'+line)
        NDA = sline[0]+"/"+sline[1]
        prodName = sline[5].strip()
        form = sline[2]
        strength = sline[3]
        status = 'Unknown'
        if strength.find(" **Federal ") > -1: #(" **Federal Register determination that product was not discontinued or withdrawn for safety or efficacy reasons**") > -1:
            status = 'Discontinued FR'
            strength = strength[:strength.find(" **")]
        for ingred in parseIngred(sline[6], uniiPT, uniiALL, missing):
            if not UNII2prods.has_key(ingred):
                UNII2prods[ingred] = []
            UNII2prods[ingred].append(NDA)
        prods[NDA] = [NDA, prodName, form, strength, status]
        line = fp.readline()
    fp.close()

    # write out ingredients that don't map to UNIIs
    missingUNIIsfile = maindir+"/temp/missingUNIIs-"+getTimeStamp()+".txt"
    fp = open(missingUNIIsfile, 'w')
    m2 = []

    for key in missing.keys():
        m2.append([len(missing[key]), key, missing[key][0]])
    m2.sort()
    m2.reverse()
    fp.write("Number of products\tIngredient\tExample NDA\n")

    for item in m2:
        fp.write(str(item[0])+"\t"+item[1]+"\t"+item[2]+"\n")
    fp.close()

    print "Prods in memory:", len(prods)

    # read in marketing status
    fp = zfp.open('MarketingStatus.txt', 'r')
    markt = readTabFP(fp)
    fp.close()
    sl = dict()
    sl['1'] = 'Prescription'
    sl['2'] = 'Over-the-counter'
    sl['3'] = 'Discontinued'
    sl['4'] = 'None (Tentative Approval)'

    #MarketingStatusID	ApplNo	ProductNo
    for entry in markt['table']:
        key = entry[1]+"/"+entry[2]
        status = sl[entry[0]]
        if prods.has_key(key) and prods[key][-1] != 'Discontinued FR':
            prods[key][-1] = status
        #else:
        #    print key, status

    # read in sponsor and application type
    fp = zfp.open('Applications.txt', 'r')
    appInfo = readTabFP(fp)
    fp.close()

    #ApplNo	ApplType	ApplPublicNotes	SponsorName
    #0          1               2               3
    apps = dict()
    for entry in appInfo['table']:
        apps[entry[0]] = entry

    for key in prods.keys():
        if apps.has_key(key[0:6]):
            prods[key].append(apps[key[0:6]][1])
            prods[key].append(apps[key[0:6]][3])
        else:
            prods[key].append('')
            prods[key].append('')

    # read in submission dates
    fp = zfp.open('Submissions.txt', 'r')
    submDates = readTabFP(fp)
    fp.close()
    #ApplNo	SubmissionClassCodeID	SubmissionType	SubmissionNo	SubmissionStatus	SubmissionStatusDate	SubmissionsPublicNotes	ReviewPriority
    #0          1                       2               3               4                       5                       6                       7
    subm = dict()

    for entry in submDates['table']:
        if not subm.has_key(entry[0]):
            subm[entry[0]] = entry[5][0:10]
        elif subm[entry[0]] > entry[5][0:10]:
            subm[entry[0]] = entry[5][0:10]

    for key in prods.keys():
        if subm.has_key(key[0:6]):
            prods[key].append(subm[key[0:6]])
            prods[key].append('Drugs@FDA')
        else:
            prods[key].append('')
            prods[key].append('')

    zfp.close()

    # read in Orange Book products
    zfp = zipfile.ZipFile(obfile, 'r')
    fp = zfp.open('products.txt', 'r')
    obprods = readTabFP(fp, True, '~')
    fp.close()

    #Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~Approval_Date~RLD~RS~Type~Applicant_Full_Name
    #0          1        2          3         4        5         6       7          8       9             10  11 12   13
    sponsors = dict()
    appTypes = dict()
    appTypes['N'] = 'NDA'
    appTypes['A'] = 'ANDA'
    sl['RX'] = 'Prescription'
    sl['OTC'] = 'Over-the-counter'
    sl['DISCN'] = 'Discontinued'

    for entry in obprods['table']:
        appl = entry[6]+'/'+entry[7]
        # other product info is updated
        ts = time.strptime("Jan 1, 1982", "%b %d, %Y")
        if entry[9] != "Approved Prior to Jan 1, 1982":
            ts = time.strptime(entry[9], "%b %d, %Y")
        date = time.strftime("%Y-%m-%d", ts)
        status = sl[entry[12]]
        if entry[4].find(" **Federal ") > -1: # (" **Federal Register determination that product was not discontinued or withdrawn for safety or efficacy reasons**") > -1:
            status = 'Discontinued FR'
            entry[4] = entry[4][:entry[4].find(" **")]
        sponsors[entry[3].strip()] = entry[13]

        # verify ingredients are mapped to this product
        for ingred in parseIngred(entry[0], uniiPT, uniiALL, missing):
            if appl not in prods:
                prods[appl] = [appl, entry[2], entry[1], entry[4], status, appTypes[entry[5]], entry[3], date, 'OrangeBook']   #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']
                print "added orange book prod:", prods[appl]
            if not UNII2prods.has_key(ingred):
                UNII2prods[ingred] = []
                #raise ValueError('Ingredient from Orange Book not found in products:'+ingred)
            if appl not in UNII2prods[ingred]:
                UNII2prods[ingred].append(appl)
                #raise ValueError('Product number from Orange Book unexpectedly mapped to unii:'+appl+":"+ingred)

        if prods.has_key(appl):
            # I've verified manually that these differences are not important
            #stop = -1
            #if prods[appl][1] != entry[2].strip():
            #    stop = 0
            #if prods[appl][2] != entry[1]:
            #    stop = 1
            #if prods[appl][3] != entry[4]:
            #    stop = 2
            if prods[appl][4] != status and status == 'Discontinued FR':
                prods[appl][4] = status
            #elif prods[appl][4] != status:
            #    stop = 3
            if prods[appl][5] == '':
                prods[appl][5] = appTypes[entry[5]]
            #elif prods[appl][5] != appTypes[entry[5]]:
            #    stop = 4
            if prods[appl][6] == '':
                prods[appl][6] = entry[3].strip()
            #elif prods[appl][6] != entry[3].strip():
            #    stop = 5
            if prods[appl][7] == '' or prods[appl][7] > date:
                prods[appl][7] = date
                prods[appl][8] = 'OrangeBook'
            #if stop > -1:
            #    print stop, appl, prods[appl][stop]
            #    print prods[appl]
            #    print entry
            #    sys.exit()
        else:
            print appl
            print entry
            raise ValueError('Product in Orange Book not found in products:'+appl)

    #prods has format: key="NDA/prodno" : [0:NDA/prodno, 1:Prod name, 2:Form;Doseage, 3:Strength, 4:Availability (Prescription; Over-the-counter; Discontinued), 5:Appl type (NDA), 6:Sponsor, 7:Marketing startDate, 8: Marketing startDate ref]

    # read in NME prod dates file
    #Year	Trademark	Generic Name	UNII	Date [mm-dd-yy]	App Type	NDA/BLA No.	Sponsor	Date Ref
    #0          1               2               3       4               5               6               7       8
    fdaNMEs = readTabFile(fdaNMEfile)
    fdaNMEdates = dict()

    for entry in fdaNMEs['table']:
        unii = entry[3]
        year = entry[0]
        date = year+"-12-31"
        if entry[4] != '':
            ts = time.strptime(entry[4], "%m-%d-%y")
            date = time.strftime("%Y-%m-%d", ts)
            if date[0:4] != year:
                date = year + date[4:]
        dateRef = entry[8].decode('latin-1').encode('ascii', 'replace')
        product = entry[1]
        sponsor = entry[7]
        appNo = entry[6]
        if appNo == '':
            appNo = product+" ("+year+") " + sponsor
        else:
            appNo = appNo[:len(appNo)-4]+appNo[-3:]
            while len(appNo) < 6:
                appNo = "0" + appNo
            fdaNMEdates[appNo] = [date, dateRef]
            appNo =  appNo + " " + product+" ("+year+") " + sponsor
        appType = entry[5]
        prod = [appNo, product, '', '', '', appType, sponsor, date, dateRef]
        prods[appNo] = prod
        if not UNII2prods.has_key(unii):
            UNII2prods[unii] = []
        UNII2prods[unii].append(appNo)
    for prod in prods.keys():
        if prod[0:6] in fdaNMEdates:
            nmeDate = fdaNMEdates[prod[0:6]]
            date = prods[prod][-2]
            method = prods[prod][-1]
            if date == '' or (date == '1982-01-01' and method == 'OrangeBook'):
                prods[prod][-2] = nmeDate[0]
                prods[prod][-1] = nmeDate[1]
            elif nmeDate[0] < date: # we should go back and curate these!
                #print "different dates:",startDate, nmeDate, prods[prod]
                prods[prod][-2] = nmeDate[0]
                prods[prod][-1] = nmeDate[1]

    # TOO MUCH TROUBLE
    # fix 1982-01-01 OrangeBook dates with regression
    #prods = apprDateRegression(prods)

    # use predicted apprv startDate from previous approvalYears file
    appYrs = readTabFile(appYrsfile)
    appPredDate = dict()

    for entry in appYrs['table']:
        unii = entry[0]
        ts = time.strptime(entry[5], "%m/%d/%Y")
        date = time.strftime("%Y-%m-%d", ts)
        method = entry[6]
        appl = entry[4][entry[4].find(' ')-6:entry[4].find(' ')]
        if method == 'PREDICTED':
            appPredDate[appl] = date

    for prod in prods.keys():
        if prod[0:6] in appPredDate:
            pred = appPredDate[prod[0:6]]
            date = prods[prod][-2]
            method = prods[prod][-1]
            if date == '' or (date == '1982-01-01' and method == 'OrangeBook') or pred < date: # these should be further curated!
                prods[prod][-2] = pred
                prods[prod][-1] = 'PREDICTED'
                #print prods[prod]

    # get active moieties from tyler's dump file
    activeMoiety = dict()
    with gzip.open( gsrsDumpfile, 'rb') as fp:
        line = fp.readline()
        while line != "":
            r = json.loads(unicode(line, errors='ignore'))
            addrel = 0
            for rel in r['relationships']:
                if rel['type'] == 'ACTIVE MOIETY':
                    addrel = 1
                    if not activeMoiety.has_key(rel['relatedSubstance']['approvalID']):
                        activeMoiety[rel['relatedSubstance']['approvalID']] = []
                    activeMoiety[rel['relatedSubstance']['approvalID']].append(r['approvalID'])
            if addrel == 0:
                if not activeMoiety.has_key(r['approvalID']):
                    activeMoiety[r['approvalID']] = []
                activeMoiety[r['approvalID']].append(r['approvalID'])
            line = fp.readline()
    print "read unii dump file"

    # validate against previous approvalYears file
    appYrs = readTabFile(appYrsfile)
    for entry in appYrs['table']:
        unii = entry[0]
        ts = time.strptime(entry[5], "%m/%d/%Y")
        date = time.strftime("%Y-%m-%d", ts)
        method = entry[6]
        appl = entry[4][entry[4].find(' ')-6:entry[4].find(' ')]
        match = 5
        if method == 'PREDICTED':
            early = [getTimeStamp(), 'Not available']
            if not activeMoiety.has_key(unii): # active moiety was recently updated
                for key in activeMoiety.keys():
                    for item in activeMoiety[key]:
                        if item == unii:
                            unii = key

            for otherunii in activeMoiety[unii]:
                if UNII2prods.has_key(otherunii):
                    for prod in UNII2prods[otherunii]:
                        entry2 = prods[prod]

                        if entry2[-2] != '' and entry2[-2] < early[-2]:
                            early = entry2

            if date == early[-2] or date > early[-2]:
                match = 0 # startDate is the same or earlier

            if date < early[-2]:
                for otherunii in activeMoiety[unii]:
                    if UNII2prods.has_key(otherunii):
                        for prod in UNII2prods[otherunii]:
                            if prod[0:6] == appl:
                                match = 4 # appl is in list and startDate doesn't work
                if match == 5:
                    for prod in prods.keys():
                        if prod[0:6] == appl:
                            match = 1 # appl exists and wasn't mapped to this unii
            if match > 1:
                print date, unii, method, appl

                print activeMoiety[unii]

                for otherunii in activeMoiety[unii]:
                    if UNII2prods.has_key(otherunii):
                        for prod in UNII2prods[otherunii]:
                            print otherunii, prod, prods[prod]

                for prod in prods.keys():
                    if prod[0:6] == appl:
                        print prod, prods[prod]

                for otherunii in UNII2prods.keys():
                    for key in UNII2prods[otherunii]:
                        if key[0:6] == appl:
                            print appl, otherunii

                print early
                raise ValueError('Tyler had other info here:'+appl)

    # write out new approval years file
    outfile =  maindir+"/data/approvalYears-"+getTimeStamp()+".txt"
    fp = open(outfile, 'w')
    #header = "UNII\tApproval\tYear\tUnknown\tComment\tDate\tDate Method\n"
    header = "UNII\tApproval_Year\tDate\tDate_Method\tApp_Type\tApp_No\tSponsor\tProduct\tUrl\tactive\tComment\n"
    fp.write(header)

    #outfile2 =  maindir+"/temp/additionalWithdrawn-"+getTimeStamp()+".txt"
    #fperr = open(outfile2, 'w')
    #fperr.write(header)
    for unii in activeMoiety.keys():
        # early = [getTimeStamp(), '']
        # earlyDate = getTimeStamp()
        #
        # for otherunii in activeMoiety[unii]:
        #     if UNII2prods.has_key(otherunii):
        #         for prod in UNII2prods[otherunii]:
        #             entry = prods[prod]
        #
        #             if entry[-2] < "1938-08-01":
        #                 writeInitApp(fp, unii, entry, entry[-2])
        #             elif entry[-1] == 'Drugs@FDA' or entry[-1] == 'OrangeBook':
        #                 if early[-1] == '' or (entry[-2] != '' and not entry[-2] > early[-2]):
        #                     early = entry
        #             elif early[-1] == '':
        #                 early = entry
        #
        #             if entry[-2] != '' and entry[-2] < earlyDate and entry[-2] > "1938-08-01":
        #                 earlyDate = entry[-2]
        #
        early = dict()
        earlyDate = getTimeStamp()

        for otherunii in activeMoiety[unii]:
            if UNII2prods.has_key(otherunii):
                for prod in UNII2prods[otherunii]:
                    entry = prods[prod]

                    akey = entry[0][0:6]
                    if not early.has_key(akey):
                        early[akey] = entry
                    else: # merge records
                        if early[akey][-2] == '' or (entry[-2] != '' and early[akey][-2] > entry[-2]):
                            early[akey] = entry

                    #if entry[-2] < "1938-08-01":
                    #    early[entry[-1]] = entry
                    #elif entry[-1] == 'Drugs@FDA' or entry[-1] == 'OrangeBook':
                    #    if not early.has_key(entry[-5]) or (entry[-2] != '' and not entry[-2] > early[entry[-5]][-2]):
                    #        early[entry[-5]] = entry
                    #elif not early.has_key(entry[-5]):
                    #    early[entry[-5]] = entry

                    if entry[-2] != '' and entry[-2] < earlyDate and entry[-2] > "1938-08-01":
                        earlyDate = entry[-2]

        for key in early.keys():
            if early[key][-2] != '' and early[key][-2][0:4] == earlyDate[0:4] and early[key][-2] > earlyDate:
                early[key][-2] = earlyDate
            writeInitApp(fp, unii, early[key], early[key][-2])
            # date = earlyDate[5:7]+"/"+earlyDate[8:]+"/"+earlyDate[0:4]
            # year = date[-4:]
            # method = early[-1]
            # apptype = early[-4]
            # appsponsor = early[-3]
            # product = early[-8]
            # appno = ''
            # appurl = ''
            # active = "true"
            # outfp = fp
            #
            # if len(early[0]) > 6 and early[0][6] == '/' or early[-1].find('https://www.accessdata.fda.gov/') > -1:
            #     appno = early[0][0:6]
            #
            #     if method == "PREDICTED":
            #         year = "[Approval Date Uncertain] "+year
            #
            #     appurl = "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo="+appno
            # else:
            #     #outfp = fperr
            #     active = "false"
            #     if method.find('OB NME Appendix 1950-') > -1:
            #         appno = early[0][0:6]
            #
            #     for otherunii in activeMoiety[unii]:
            #         if UNII2prods.has_key(otherunii):
            #             for prod in UNII2prods[otherunii]:
            #                 entry = prods[prod]
            #                 if entry[0] == early[0]:
            #                     unii = otherunii
            #
            # #comment = apptype+'|'+appno+'|'+appsponsor+'|'+product+'|'+appurl+'|'
            # #comment = comment + early[0].decode('latin-1').encode('ascii', 'replace')
            # #outline = unii+"\tApproval Year\t"+year+"\t\t"+comment+"\t"+date+"\t"+method+"\n"
            # comment = early[0].decode('latin-1').encode('ascii', 'replace')
            # outline = unii+"\t"+year+"\t"+date+"\t"+method+"\t"+apptype+"\t"+appno+"\t"+appsponsor+"\t"+product+"\t"+appurl+"\t"+active+"\t"+comment+"\n"
            # outfp.write(outline)

    # write out all products data
    prod2UNIIs = dict()
    for unii in UNII2prods.keys():
        for prod in UNII2prods[unii]:
            if not prod2UNIIs.has_key(prod):
                prod2UNIIs[prod] = []
            prod2UNIIs[prod].append(unii)

    UNII2pt = dict()

    for key in resolverCache.keys():
        UNII2pt[resolverCache[key]] = key

    for key in uniiPT.keys():
        UNII2pt[uniiPT[key]] = key

    productsfile = maindir+"/temp/products-"+getTimeStamp()+".txt"
    fp = open(productsfile, 'w')

    header = 'NDA\tProduct\tForm;Route\tStrength\tStatus\tAppl Type\tSponsor\tDate\tDate Ref\tUNIIs\tIngredients\n'
    fp.write(header)
    
    for prod in prods.keys():
        uniis = []
        if prod2UNIIs.has_key(prod): # ingreds that don't have uniis
            uniis = prod2UNIIs[prod]
        uniilist = '; '.join(uniis)
        ingreds = []
        for unii in uniis:
            if not UNII2pt.has_key(unii):
                print unii, uniilist, prod
                sys.exit()
            ingreds.append(UNII2pt[unii])
        ingredlist = '; '.join(ingreds)
        entry = prods[prod]
        entry.append(uniilist)
        entry.append(ingredlist)
        outline = ""
        for item in entry:
            outline = outline + item + '\t'
        fp.write(outline+'\n')
    fp.close()
