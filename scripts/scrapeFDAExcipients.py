import os
import sys
import time
import json
import string
import urllib2
import urllib
import zipfile
import gzip
import numpy
import cookielib

cookies = cookielib.CookieJar()

opener = urllib2.build_opener(
    urllib2.HTTPRedirectHandler(),
    urllib2.HTTPHandler(debuglevel=0),
    urllib2.HTTPSHandler(debuglevel=0),
    urllib2.HTTPCookieProcessor(cookies))
opener.addheaders = [
    ('User-agent', ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'))
]

def getMainDir():
    curr = os.getcwd()
    if curr[-8:] == '/scripts':
        curr = curr[:-8]
    if curr[-5:] == '/temp':
        curr = curr[:-5]

    #!!!!!!
    #if not os.path.exists(curr+"/.git") or not os.path.exists(curr+"/data") or not os.path.exists(curr+"/scripts"):
    #    raise ValueError('Could not identify repo head from current directory')

    if not os.path.exists(curr+"/temp"):
        os.mkdir(curr+"/temp")
    
    return curr

def requestJson(uri):
    try:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()
        obj = json.loads(response)
        return obj
    except:
        sys.stderr.write("failed: "+uri+"\n")
        sys.stderr.flush()
        time.sleep(5)

def resolveName(name):
    name = name.encode('ascii', 'ignore')
    unii = ""
    if name in defaultNames.keys():
        unii = defaultNames[name]
    else:
        unii = resolveNameTripod(str(name)+" [GREEN BOOK]")
        if unii == "":
            unii = resolveNameTripod(name)
    if unii == "":
        sys.stderr.write("Can not resolve name to a unii: "+name+"\n")
    return unii

def getTimeStamp():
    ts = time.gmtime()
    return time.strftime("%Y-%m-%d", ts)

def getUNIIZipURL():
    zipurl = "https://fdasis.nlm.nih.gov/srs/download/srs/UNIIs.zip"
    return zipurl

def getUNIIlist(maindir):

    uniifile = maindir+"/temp/UNIIs-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+uniifile + " " + getUNIIZipURL()
    print syscall
    if not os.path.exists(uniifile):
        os.system(syscall)

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
    
    return uniiPT, uniiALL

def readDelLine(line, sep=','):
    sline = []
    inQ = False
    i = 0
    j = 0
    while i < len(line):
        if line[i] == '"':
            inQ = not inQ
        elif line[i] == sep and not inQ:
            sline.append(line[j:i])
            j = i + 1
        i = i + 1
    sline.append(line[j:-2])
    return sline

if __name__=="__main__":

    maindir = getMainDir()
    #uniiPT, uniiALL = getUNIIlist(maindir)

    iig = dict()
    iig[''] = dict()
    iig['N/A'] = dict()

    # get all iig files
    iigfiles = dict()
    uris = ['https://www.fda.gov/Drugs/InformationOnDrugs/ucm113978.htm', 'http://wayback.archive-it.org/7993/20170112022245/http:/www.fda.gov/Drugs/InformationOnDrugs/ucm113978.htm']
    for uri in uris:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()

        sresp = response.split("\n")
        for line in sresp:
            if line.find("Inactive Ingredients Database Download File") > -1:
                dbURL = line[line.find('"')+1:]
                dbURL = "https://www.fda.gov"+dbURL[:dbURL.find("\"")]
                ts = getTimeStamp()
                iigfile = maindir+"/temp/current-iig-"+ts+".zip"
                syscall = "curl --insecure -o "+iigfile + " " + dbURL
                print syscall
                if not os.path.exists(iigfile):
                    os.system(syscall)

                iigfiles[ts] = iigfile
                print ts, iigfile
            elif line.find("Inactive Ingredient Database File") > -1:
                dbURL = line[line.find('"')+1:]
                if dbURL[0:3] == "UCM":
                    dbURL = "http://wayback.archive-it.org/7993/20170112022245/http://www.fda.gov/downloads/Drugs/InformationOnDrugs/"+dbURL+".zip"
                else:
                    dbURL = uri[:uri.find("/", 8)]+urllib.quote(dbURL[:dbURL.find("\"")])
                ts = "null"
                try:
                    ts = time.strftime("%Y-%m-%d", time.strptime(line[line[:line.find(': ')].rfind('>')+1:line.find(': ')], '%B %Y'))
                except:
                    try:
                        ts = time.strftime("%Y-%m-%d", time.strptime(line[line[:line.find(': Inactive')].rfind('\"')+1:line.find(': Inactive')], '%B %Y'))
                    except:
                        ts = time.strftime("%Y-%m-%d", time.strptime(line[line[:line.find(' Inactive')].rfind('>')+1:line.find(' Inactive')], '%B %Y'))
                iigfile = maindir+"/temp/iig-"+ts+".zip"
                syscall = "curl --insecure -v -L -o "+iigfile + " " + dbURL
                print syscall
                if not os.path.exists(iigfile):
                    os.system(syscall)

                iigfiles[ts] = iigfile
                print ts, iigfile

    # load current iig file
    iigfile = iigfiles[getTimeStamp()]
    zfp = zipfile.ZipFile(iigfile, 'r')
    names = zfp.namelist()
    csvfile = names[0]
    for item in names[1:]:
        if item.find(".csv") > -1:
            csvfile = item
    fp = zfp.open(csvfile, 'r')
    line = fp.readline()
    header = "INGREDIENT_NAME,ROUTE,DOSAGE_FORM,CAS_NUMBER,UNII,POTENCY_AMOUNT,POTENCYUNIT"
    if line[:len(header)] != header:
        raise ValueError('Problem reading IIG file:'+line)

    shead = header.split(",")
    line = fp.readline()
    while line != "":
        sline = readDelLine(line)
        if len(sline) < len(shead):
            raise ValueError('Wrong number of items in iig entry: '+len(sline)+":"+len(shead))
        entry = dict()
        for i in range(len(shead)):
            if len(sline[i]) > 0 and sline[i][0] == "\"" and sline[i][-1] == "\"":
                sline[i] = sline[i][1:-1]
            entry[shead[i]] = sline[i]
        rec = sline[shead.index('UNII')]
        if not iig.has_key(rec):
            iig[rec] = []
        if rec != "":
            iig[rec].append(entry)
        line = fp.readline()
    fp.close()
    zfp.close()

    # check old iig files
    keys = iigfiles.keys()
    del keys[keys.index(getTimeStamp())]
    keys.sort()
    keys.reverse()
    for key in keys:
        if key != getTimeStamp() and os.path.isfile(iigfiles[key]):
            #print key
            zfp = zipfile.ZipFile(iigfiles[key], 'r')
            names = zfp.namelist()
            csvfile = ""
            sep = ','
            shead = header.split(",")
            items = len(shead)
            for item in names:
                if item.find(".csv") > -1:
                    csvfile = item
                elif item.find("iig-Internet.txt") > -1:
                    csvfile = item
                    sep = '\t'
                elif item.find(".txt") > -1:
                    csvfile = item
                    sep = '~'
            if csvfile == "":
                continue
            fp = zfp.open(csvfile, 'r')
            line = fp.readline()
            line = line.replace(sep, ",")
            if sep != '~' and line[:min(len(header),len(line)-2)].upper() != header[:min(len(header),len(line)-2)].upper():
                raise ValueError('Problem reading IIG file:\n'+line+"\n"+header)
            
            line = fp.readline()
            count = 0
            while line != "":
                sline = readDelLine(line, sep)
                if sep == '~':
                    sline.insert(2, (sline[1].split(";  "))[-1])
                    sline.insert(2, (sline[1].split(";  "))[0])
                    del sline[1]
                if len(sline) < items:
                    raise ValueError('Wrong number of items in iig entry: '+str(len(sline))+":"+str(len(shead)))
                    items = min(len(sline), len(shead))
                entry = dict()
                for i in range(items):
                    entry[shead[i]] = sline[i]
                rec = sline[shead.index('UNII')]
                if not iig.has_key(rec):
                    print "Missing unii entry:", key, rec, entry
                    iig[rec] = [] # don't actually save/use this info yet - requires review
                elif rec == '1':
                    found = False
                    uniq = entry['UNII']# + ":" + entry['DOSAGE_FORM']
                    for item in iig[rec]:
                        uniq2 = item['UNII']# + ":" + item['DOSAGE_FORM']
                        if uniq2 == uniq:
                            found = True
                    if not found:
                        print "not found"
                        print key
                        print entry
                        sys.exit()
                line = fp.readline()
                count = count + 1
            fp.close()
            zfp.close()
            #print count

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
    #header = "INGREDIENT_NAME,ROUTE,DOSAGE_FORM,CAS_NUMBER,UNII,POTENCY_AMOUNT,POTENCYUNIT"
    outfile =  maindir+"/data/FDAexcipients-"+getTimeStamp()+".txt"
    fp = open(outfile, 'w')
    header = "UNII\tIIG-ID\tRoute\tStrength\tIIG-URL\n"
    fp.write(header)
    for unii in iig.keys():
        for entry in iig[unii]:
            id = entry['INGREDIENT_NAME']+":"+entry['ROUTE']+":"+entry['DOSAGE_FORM']+":"+entry['POTENCYUNIT']
            route = entry['ROUTE']
            strength = entry['POTENCY_AMOUNT'] + entry['POTENCYUNIT']
            if strength == "NA":
                strength = ""
            elif len(strength) > 0 and len(entry['ROUTE']) > 0:
                strength = strength + " "
            strength = strength + entry['ROUTE']
            if len(strength) > 0 and len(entry['DOSAGE_FORM']) > 0:
                strength = strength + " "
            strength = strength + entry['DOSAGE_FORM']
            url = "https://www.accessdata.fda.gov/scripts/cder/iig/index.cfm"
            ing = entry['INGREDIENT_NAME']
            if ing[0].isalpha() or ing[0].isdigit():
                url = url + "?event=browseByLetter.page&Letter=" + ing[0]
            elif ing[1].isalpha():
                url = url + "?event=browseByLetter.page&Letter=" + ing[1] 
            outline = unii + "\t" + id + "\t" + route + "\t" + strength + "\t" + url + "\n"
            fp.write(outline)
    fp.close()
            
            
