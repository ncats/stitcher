import os
import sys
import time
import json
import urllib2
import zipfile

resolverCache = dict()
resolverCache["PEMETREXED SODIUM"] = "2PKU919BA9"
resolverCache["NETARSUDIL DIMESYLATE"] = "VL756B1K0U"
resolverCache["INSULIN LISPRO RECOMBINANT"] = "GFX7QIS1II"
resolverCache["INSULIN SUSP ISOPHANE RECOMBINANT HUMAN"] = "1Y17CTI5SR"
resolverCache["PITAVASTATIN MAGNESIUM"] = "M5681Q5F9P"
resolverCache["IVACAFTOR, TEZACAFTOR"] = "8RW88Y506K"
resolverCache["NITROGEN, NF"] = "N762921K75"
resolverCache["ELEXACAFTOR, IVACAFTOR, TEZACAFTOR"] = "RRN67GMB0V"
resolverCache["GALLIUM DOTATOC GA-68"] = "Y68179SY2L"
resolverCache["GALLIUM DOTATATE GA-68"] = "9L17Y0H71P"
resolverCache["SIPONIMOD FUMARIC ACID"] = "Z7G02XZ0M6"
resolverCache["OMEGA-3ACID ETHYL ESTERS"] = "D87YGH4Z0Q"
resolverCache["OMEGA-3-ACID ETHYL ESTERS TYPE A"] = "D87YGH4Z0Q"
resolverCache["GRISEOFULVIN, ULTRAMICROSIZE"] = "32HRV3E3D5"
resolverCache["FISH OIL TRIGLYCERIDES"] = "XGF7L72M0F"
resolverCache["VERARD"] = "A7V27PHC7A" #https://books.google.com/books?id=IXWOXUpylO8C&pg=PA47&lpg=PA47&dq=verard+drug&source=bl&ots=WYI8qC1dDL&sig=ACfU3U3fGdeaopIKKyPWB3agXtKCHqNb9g&hl=en&sa=X&ved=2ahUKEwiwlc3pobnmAhVIwVkKHZ6lCIAQ6AEwCHoECAoQAQ#v=onepage&q=verard%20drug&f=false
resolverCache["COAGULATION FACTOR XA (RECOMBINANT), INACTIVATED - ZHZO"] = "BI009E452R"
resolverCache["FAM-TRASTUZUMAB DERUXTECAN-NXKI"] = "5384HK7574"

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

def getDateStamp():
    ts = time.gmtime()
    return time.strftime("%Y-%m-%d", ts)

def getUNIIZip():
    zipurl = "https://fdasis.nlm.nih.gov/srs/download/srs/UNIIs.zip"
    ts = time.gmtime()
    dateStamp = time.strftime("%Y-%m-%d", ts)
    uniiFile = ""
    for file in os.listdir(getMainDir()+"/temp"):
        if file[0:6] == "UNIIs-" and file[-4:] == ".zip":
            if (time.mktime(ts) - time.mktime(time.strptime(file, "UNIIs-%Y-%m-%d.zip")))/3600/24 < 90:
                uniiFile = file

    if uniiFile == "":
        uniifile = getMainDir()+"/temp/UNIIs-"+getDateStamp()+".zip"
        syscall = "curl --insecure -o "+uniiFile + " " + zipurl
        print(syscall)

        if not os.path.exists(uniiFile):
            os.system(syscall)

    return uniiFile

def readUNIIs(uniifile):
    zfp = zipfile.ZipFile(uniifile, 'r')
    names = zfp.namelist()
    sys.stderr.write((names[-1]+"\n"))
    fp = zfp.open(names[-1], 'r')
    line = fp.readline().decode('ascii')

    if line[:-2] != 'Name\tType\tUNII\tDisplay Name':
        raise ValueError('Problem reading UNII file:'+str(line))

    line = fp.readline().decode('ascii')
    uniiPT = dict()
    uniiALL = dict()

    while line != "":
        sline = line[:-2].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading UNII fileline:'+str(line))
        uniiPT[sline[3]] = sline[2]
        if sline[0][-1] == "]" and sline[0].rfind(" [") > -1:
            sline[0] = sline[0][:sline[0].rfind(" [")]
        uniiALL[sline[0]] = sline[2]
        line = fp.readline().decode('ascii')
    sys.stderr.write("UNIIs in memory:"+str(len(uniiPT))+" "+str(len(uniiALL))+"\n")
    sys.stderr.flush()
    return uniiPT, uniiALL

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


uniifile = getUNIIZip()
uniiPT, uniiALL = readUNIIs(uniifile)
