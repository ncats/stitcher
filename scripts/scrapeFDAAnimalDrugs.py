import os
import io
import sys
import cookielib
import urllib
import urllib2
import json
import time
import ssl
import pandas

cookies = cookielib.CookieJar()

gcontext = ssl._create_unverified_context() #ssl.SSLContext()  # Only for gangstars
opener = urllib2.build_opener(
    urllib2.HTTPRedirectHandler(),
    urllib2.HTTPHandler(debuglevel=0),
    urllib2.HTTPSHandler(debuglevel=0, context=gcontext),
    urllib2.HTTPCookieProcessor(cookies))
opener.addheaders = [
    ('User-agent', ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'))
]

site = 'https://animaldrugsatfda.fda.gov/'

directory = "../data/"
try:
    os.stat(directory+"NADAs.json")
except:
    directory = "data/"
    try:
        os.stat(directory+"NADAs.json")
    except:
        sys.stderr.write("Can't figure out where the data files are!\n")
        sys.exit()
FDAanimalDrugstxt = "FDAanimalDrugs.txt"
oldnadasjson = directory+"NADAs.json"
newnadasjson = "NADAs.json"
faradProds = directory+"faradProds.txt"
faradtxt = directory+"farad.txt"

def requestJson(uri):
    try:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()
        obj = json.loads(response)
        return obj
    except Exception, err:
        print Exception, err
        sys.stderr.write("failed: "+uri+"\n")
        sys.stderr.flush()
        time.sleep(5)

def requestHtml(uri):
    try:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()
        return response
    except Exception, err:
        print Exception, err
        sys.stderr.write("failed: "+uri+"\n")
        sys.stderr.flush()
        time.sleep(5)

def exciseTd(l):
    l = l[l.find("<td ")+4:]
    l = l[l.find(">")+1:]
    l = l[:l.find("</td>")]
    while l.find("<") > -1:
        l = l[:l.find("<")]+l[l.find(">", l.find("<"))+1:]
    l = l.strip()
    return l
        
def scrapeEachFarad():
    drugs = dict()
    fp = open(faradtxt, "r")
    lines = fp.readlines()
    fp.close()
    for line in lines:
        obj = json.loads(line)
        app = obj['NADA/ANADA']
        print app

        if drugs.has_key(app):
            drug = drugs[app]
            for key in obj.keys():
                if drug.has_key(key) and drug[key] != obj[key] and obj[key] not in drug[key]:
                    drug[key] = drug[key] + "|" + obj[key]
            print json.dumps(drug, ensure_ascii=False, encoding='utf8')
        else:
            uri = "http://www.farad.org/vetgram/ProductInfo.asp?byNada="+app
            fp = urllib.urlopen(uri)
            response = fp.read().split("\n")
            fp.close()

            for i in range(len(response)):
                if response[i].find('<td width="13%" valign="top"><strong>Manufacturer:</strong></td>') > -1:
                    manu = response[i+2][response[i+2].find("<td>")+4:response[i+2].find("</td>")]
                    if manu.find("</a>") > -1:
                        manu = manu[manu.find(">")+1:manu.find("</a>")]
                        manu = manu.replace("&nbsp;", "")
                    obj['Manufacturer'] = manu
                if response[i].find('<td valign="top"><strong>Formulation:</strong></td>') > -1:
                    obj['Formulation'] = response[i+2][7:-6]
                if response[i].find('<td><strong>Drug Class:</strong></td>') > -1:
                    obj['DrugClass'] = response[i+2].strip()
                if response[i].find('<td><strong>Dose Form:</strong></td>') > -1:
                    obj['DoseForm'] = response[i+2].strip()
            drugs[app] = obj
            time.sleep(3)

    jsonFile = io.open(faradProds, 'w', encoding='utf8')
    for drug in drugs.keys():
        data = json.dumps(drugs[drug], ensure_ascii=False, encoding='utf8')
        jsonFile.write(data)
        jsonFile.write(unicode("\n"))
    jsonFile.close()

def scrapeFarad():
    jsonFile = io.open(faradtxt, 'w', encoding='utf8')
    uri = "http://www.farad.org/vetgram/searchlist.asp"
    routes = [1, 640, 5, 2, 'other', 3, 620, 'topical']
    drugs = []
    for route in routes:
        headers = ['NADA/ANADA', 'Trade Name', 'Active Ingredient', 'Species', 'Route', 'Drug Type', 'Rx', 'Market Status']
        data = {'IsOtherSp': '', 'IsMarketed': 'ON', 'c_route': route}
        fp = urllib.urlopen(uri, urllib.urlencode(data))
        lines = fp.read().split("\n")
        fp.close()
        sets = []
        for i in range(len(lines)):
            if lines[i].find('<form method="POST" action="vetgram/ProductInfo.asp">') > -1:
                sets.append(i)
        for i in range(len(sets)):
            prod = lines[sets[i]+1:]
            if i < len(sets)-1:
                prod = lines[sets[i]+1:sets[i+1]]
            entry = []
            for item in prod:
                if item.find("<td ") > -1:
                    entry.append(exciseTd(item))
            adict = dict()
            for j in range(len(headers)):
                adict[headers[j]] = entry[j]
            drugs.append(adict)
    for drug in drugs:
        data = json.dumps(drug, ensure_ascii=False, encoding='utf8')
        jsonFile.write(data)
        jsonFile.write(unicode("\n"))
    jsonFile.close()

def faradRoutes():
    routes = dict()
    routes['AU']='Auricular (Otic); administration to or by way of the ear.'
    routes['BUC']='Buccal; referring to the inside lining of the cheeks and is part of the lining mucosa.'
    routes['EPI']='Epidural; administration upon or over the dura mater.'
    routes['IA']='Intra-arterial; administration within an artery or arteries.'
    routes['IART']='Intra-articular; administration within a joint.'
    routes['ICAR']='Intracardiac; administration with the heart.'
    routes['IDUC']='Intraductal; administration within the duct of a gland.'
    routes['IFOL']='Intrafollicular; administered within the follicle.'
    routes['IGAS']='Intragastric; administration within the stomach.'
    routes['ILES']='Intralesional; administration within or introduced directly into a localized lesion.'
    routes['IM']='Intramuscular; administration within a muscle.'
    routes['IMAM']='Intramammary; administered within the teat canal.'
    routes['IMRS']='Immersion; administered by immersing completely in solution.'
    routes['INF']='Infiltration; administered in a diffuse pattern to multiple sites.'
    routes['INH']='Respiratory (Inhalation); administration within the respiratory tract by inhaling orally or nasally for local or systemic effect.'
    routes['IP']='Intraperitoneal; administration within the peritoneal cavity.'
    routes['ISIN']='Intrasinal; administration within the nasal or periorbital sinuses.'
    routes['ISYN']='Intrasynovial; administration within the synovial cavity of a joint.'
    routes['IT']='Intrathecal; administration within the cerebrospinal fluid at any level of the cerebrospinal axis, including injection into the cerebral ventricles.'
    routes['ITES']='Intratesticular; administration within the testicle.'
    routes['IU']='Intrauterine; administration within the uterus.'
    routes['IV']='Intravenous; administration within or into a vein or veins.'
    routes['IVES']='Intravesical; administration within the bladder.'
    routes['NAS']='Nasal; administration to the nose; administered by way of the nose.'
    routes['NG']='Nasogastric; administration through the nose and into the stomach, usually by means of a tube.'
    routes['OU']='Ophthalmic; administration to the external eye.'
    routes['PAR']='Parenteral; administration by injection, infusion, or implantation.'
    routes['PNEU']='Perineural; administration surrounding a nerve or nerves.'
    routes['PO']='Oral; administration to or by way of the mouth.'
    routes['PV']='Vaginal; administration into the vagina.'
    routes['REC']='Rectal; administration to the rectum.'
    routes['SC']='Subcutaneous; administration beneath the skin; hypodermic. Synonymous with the term SUBDERMAL.'
    routes['SCi']='Subcutaneous Implant; administered by placing under the skin.'
    routes['SL']='Sublingual; administration beneath the tongue.'
    routes['TOP']='Topical; administration to a particular spot on the outer surface of the body.'
    routes['TOPp']='Topical, Pour On; administered to the skin by pouring liquid over the back.'
    routes['TD']='Transdermal; administered through the dermal layer of the skin.'
    return routes
    
def scrapeNADAs():
    
    # download all application numbers
    #homePage = requestHtml("https://animaldrugsatfda.fda.gov/adafda/views/#/search")
    sec12Page = "https://animaldrugsatfda.fda.gov/adafda/app/search/public/tradeSponsorExcelByNadaAnada/Section12byApplicationNumber"
    sec12File = "Section12byApplicationNumber.xls"
    syscall = "curl --insecure -o "+sec12File + " " + sec12Page
    print syscall
    if not os.path.exists(sec12File):
        os.system(syscall)

    sec6Page = "https://animaldrugsatfda.fda.gov/adafda/app/search/public/voluntaryWithdrawalExcel/Section6VoluntaryWithdrawal"
    sec6File = "Section6VoluntaryWithdrawal.xls"
    syscall = "curl --insecure -o "+sec6File + " " + sec6Page
    print syscall
    if not os.path.exists(sec6File):
        os.system(syscall)
    sec6txt = "Section6VoluntaryWithdrawal.txt"
    sec12txt = "Section12byApplicationNumber.txt"

    pandas.read_excel(sec12File, sheet_name=0, index=0).to_csv(path_or_buf=sec12txt, sep='\t', encoding='utf-8', index=False)
    pandas.read_excel(sec6File, sheet_name=0, index=0).to_csv(path_or_buf=sec6txt, sep='\t', encoding='utf-8', index=False)

    files = [sec12txt, sec6txt] #"Section12byApplicationNumber.txt", "Section6VoluntaryWithdrawal.txt"]
    apps = dict()
    for filename in files:
        fp = open(filename, "r")
        lines = fp.readlines()
        fp.close()
    
        for line in lines[1:]:
            sline = line.split("\t")
            appno = sline[0]
            appno = int(appno[0:3]+appno[4:])
            apps[appno] = {}


    fp = open("PB98143464.xml", "r")
    lines = fp.readlines()
    fp.close()
    pbapps = dict()
    for line in lines:
        i = line.find("-")
        while i>-1:
            if i<3:
                line = line[i+1:]
            elif i+3 > len(line):
                line = line[i+1:]
            else:
                if i==3 or line[i-4] == " ":
                    try:
                        num1 = int(line[i-3:i])
                        num2 = int(line[i+1:i+4])
                        app = int(line[i-3:i]+line[i+1:i+4])
                        #print line[i-3:i+4], app
                        if not pbapps.has_key(app):
                            pbapps[app] = 0
                        pbapps[app] = pbapps[app] + 1
                    except:
                        yo = "not an app no"
                line = line[i+1:]
            i = line.find("-")
    for app in pbapps.keys():
        if app not in apps.keys():
            #print app, pbapps[app]
            apps[app] = {}

    #https://animaldrugsatfda.fda.gov/adafda/app/search/public/application/004536
    #{"applicationNumbers":[{"applicationId":950,"applicationNumber":6019,"applicationType":"N","applicationStatus":"Voluntarily Withdrawn","publish":false,"noohDate":null,"mode":null,"pioneerApplicationNumber":0,"splSponsorName":null}]}

    jsonFile = io.open(oldnadasjson, 'r')
    for entry in json.loads(jsonFile.read()):
        app = entry['applicationNumber']
        apps[app] = entry
    jsonFile.close()
    
    uriprefix = 'https://animaldrugsatfda.fda.gov/adafda/app/search/public/application/'
    for i in range(0, 10):
        obj = requestJson(uriprefix + str(i))
        for item in obj["applicationNumbers"]:
            app = item["applicationNumber"]
            if app not in apps.keys():
                print "Oops!"
                print app
                print item
                sys.exit()
            else:
                for entry in item.keys():
                    if not apps[app].has_key(entry) or apps[app][entry] != item[entry]:
                        apps[app][entry] = item[entry]
                
    jsonFile = io.open(newnadasjson, 'w', encoding='utf8')
    jsonFile.write(unicode("[\n"))
    for app in apps.keys():
        if apps[app] != {} and apps[app].has_key('applicationId') and not apps[app].has_key('applicationPatent'):
            #print app
            idnum = apps[app]["applicationId"]
            uri = 'https://animaldrugsatfda.fda.gov/adafda/app/search/public/retrievePreviewBean/'+str(idnum)
            obj = requestJson(uri)
            if obj is not None:
                for item in obj.keys():
                    if item != 'application':
                        apps[app][item] = obj[item]
        else:
            apps[app]["applicationNumber"] = app
        data = json.dumps(apps[app], ensure_ascii=False, encoding='utf8')
        jsonFile.write(unicode(data))
        if app != apps.keys()[-1]:
            jsonFile.write(unicode(",\n"))
    jsonFile.write(unicode("]\n"))
    jsonFile.close()

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
                    #raise ValueError("conflicting UNIIs returned:"+name+"::"+request)
                    print "conflicting UNIIs returned:"+name+"::"+request
            elif r['content'][i].has_key('approvalID'):
                if unii == '':
                    unii = r['content'][i]['approvalID']
                elif unii != r['content'][i]['approvalID']:
                    #raise ValueError("conflicting UNIIs returned:"+name+"::"+request)
                    print "conflicting UNIIs returned:"+name+"::"+request
            else:
                raise ValueError("UNIIs response not properly formatted:"+name)
    return unii

def defaultNames():

    names = dict()
    names['FURAMAZONE']='0180PBK4FC'
    names['AMMONIUM CHLORIDE']='01Q9PC255D'
    names['ALBUTEROL SULFATE']='021SEF3731'
    names['OXIBENDAZOLE']='022N12KJ0X'
    names['AFOXOLANER']='02L07H6D0U'
    names['FAMPHUR']='02UOP4Z0O0'
    names['MOMETASONE FUROATE']='04201GDN4R'
    names['IPRONIDAZOLE']='045BU63E23'
    names['LAIDLOMYCIN PROPIONATE POTASSIUM']='05TAA9I0Z8'
    names['TETRACAINE']='0619F35CGV'
    names['TOLNAFTATE']='06KB629TKV'
    names['CAMBENDAZOLE']='079X63S3DU'
    names['CLODRONATE']='0813BZ6866'
    names['CARAMIPHEN EDISYLATE']='09TQU5PG95'
    names['EPSIPRANTEL']='0C1SPQ0FSR'
    names['FLUOCINOLONE ACETONIDE']='0CD5FD6S2M'
    names['N-BUTYLSCOPOLAMMONIUM BROMIDE']='0GH9JX37C8'
    names['HYDROCHLOROTHIAZIDE']='0J48LPH2TH'
    names['CAPROMORELIN']='0MQ44VUN84'
    names['SULFADIAZINE']='0N7609K889'
    names['SODIUM SULFATE']='0YPR65R21J'
    names['DEXMEDETOMIDINE HCL']='1018WH7F9I'
    names['DEXMEDETOMIDINE HYDROCHLORIDE']='1018WH7F9I'
    names['NICARBAZIN']='11P9NUA12U'
    names['METHOCARBAMOL']='125OD7737X'
    names['PRIMIDONE']='13AFD7670Q'
    names['CARBON DIOXIDE']='142M471B3J'
    names['PENICILLIN V POTASSIUM']='146T0TU1JB'
    names['DESOXYCORTICOSTERONE PIVALATE']='16665T4A2X'
    names['POTASSIUM PHOSPHATE']='CI71S98N1Z'
    names['NOVOBIOCIN']='17EC19951N'
    names['PENICILLIN G PROCAINE']='17R794ESYN'
    names['PIPERAZINE DIHYDROCHLORIDE']='17VU4Z4W88'
    names['PIPERAZINE HYDROCHLORIDE']='17VU4Z4W88'
    names['AZAPERONE']='19BV78AK7W'
    names['VITAMIN B5']='19F5HK2737'
    names['DOXYCYCLINE HYCLATE']='19XTS3T51U'
    names['CHOLECALCIFEROL']='1C6V77QF41'
    names['CHLORTETRACYCLINE BISULFATE']='1D06KZ672I'
    names['MONENSIN SODIUM']='1GS872GAFV'
    names['FORMALIN']='1HG84L3525'
    names['FERRIC OXIDE']='1K09F3G675'
    names['PENTAZOCINE LACTATE']='1P2XIB510O'
    names['ESTRADIOL MONOPALMITATE']='1Q5Y448XT0'
    names['LUFENURON']='1R754M4918'
    names['PIPERAZINE']='1RTM4PAL0V'
    names['ESTRADIOL BENZOATE']='1S4CJB5ZGN'
    names['PREDNISOLONE TERTIARY BUTYLACETATE']='1V7A1U282K'
    names['TRIAMCINOLONE']='1ZK20VI6TY'
    names['HELIUM']='206GF3GB41'
    names['AMINOPENTAMIDE HYDROGEN SULFATE']='20P9NI883O'
    names['SULFAQUINOXALINE SODIUM']='21223EPJ40'
    names['HYDROCORTISONE ACEPONATE']='2340UP1L2G'
    names['DANOFLOXACIN']='24CU1YS91D'
    names['TOCERANIB PHOSPHATE']='24F9PF7J3R'
    names['SULFAMETHIZOLE']='25W8454H16'
    names['NICOTINIC ACID']='2679MF687A'
    names['CEFADROXIL']='280111G160'
    names['LEVAMISOLE']='2880D3468G'
    names['CITRIC ACID']='2968PHW8QP'
    names['XYLAZINE']='2KFG9TP5V8'
    names['BUTORPHANOL TARTRATE']='2L7I72RUHN'
    names['CLOMIPRAMINE HYDROCHLORIDE']='2LXW0L6GWJ'
    names['ETODOLAC']='2M36281008'
    names['CEFPODOXIME PROXETIL']='2TB00A1Z7N'
    names['ALTRENOGEST']='2U0X0JA2NB'
    names['ATIPAMEZOLE HYDROCHLORIDE']='2W4279571X'
    names['ITRACONAZOLE']='304NUG5GF4'
    names['METHOXYFLURANE']='30905R8O7B'
    names['RACTOPAMINE HYDROCHLORIDE']='309G9J93TP'
    names['SULFADIMETHOXINE']='30CPC5LDEX'
    names['GRISEOFULVIN']='32HRV3E3D5'
    names['AMITRAZ']='33IAH5017S'
    names['LACTIC ACID']='33X04XA5AT'
    names['PIMOBENDAN']='34AP3BBP9T'
    names['FLUNIXIN']='356IB1O400'
    names['TRIMEPRAZINE TARTRATE']='362NW1LD6Z'
    names['ACEPROMAZINE MALEATE']='37862HP2OM'
    names['BETA-AMINOPROPIONITRILE']='38D5LJ4KH2'
    names['SEVOFLURANE']='38LVP0K73A'
    names['PRALIDOXIME CHLORIDE']='38X7XS076H'
    names['CARBOMYCIN']='3952621T3O'
    names['OLEATE SODIUM']='399SL044HN'
    names['BUTONATE']='39M9R3Q494'
    names['NITENPYRAM']='3A837VZ81Y'
    names['IMIDACLOPRID']='3BN7M937V8'
    names['MYRISTYL-GAMMA-PICOLINIUM CHLORIDE']='3D6CWI0P23'
    names['ENROFLOXACIN']='3DX3XEK1BN'
    names['TOLUENE']='3FPU23BG52'
    names['NORGESTOMET']='3L33UD42X4'
    names['CYTHIOATE']='3OOH7Q4333'
    names['EMBUTRAMID']='3P4TQG94T1'
    names['CLINDAMYCIN']='3U02EL437C'
    names['HYDROCORTISONE ACETATE']='3X7931PO74'
    names['DIATRIZOATE MEGLUMINE']='3X9MR4N98U'
    names['TESTOSTERONE']='3XMK78S47O'
    names['HYGROMYCIN B']='3XQ2233B0B'
    names['HYGROMYCIN']='3YJY415DDI'
    names['BUPRENORPHINE']='40D3SCR4GZ'
    names['DIPHENYLHYDANTOIN SODIUM']='4182431BJH'
    names['PHENYTOIN SODIUM']='4182431BJH'
    names['CHLORAL HYDRATE']='418M5916WG'
    names['ACETAZOLAMIDE SODIUM']='429ZT169UH'
    names['CEPHAPIRIN SODIUM']='431LFF7I7J'
    names['METHYLPREDNISOLONE ACETATE']='43502P7F0P'
    names['TRIPTORELIN ACETATE']='43OFW291R9'
    names['CHLORAMPHENICOL PALMITATE']='43VU4207NW'
    names['SODIUM CHLORIDE']='451W47IQ8X'
    names['ZOLAZEPAM HYDROCHLORIDE']='45SJ093Q1N'
    names['NIFUROXIME']='465N7P5U85'
    names['MECLOFENAMIC ACID']='48I5LU4ZWD'
    names['SULFAMETHAZINE']='48U51W007F'
    names['GUAIFENESIN']='495W7451VQ'
    names['THIOPENTAL SODIUM']='49Y44QZL70'
    names['PROGESTERONE']='4G7DS2Q64Y'
    names['DICLOXACILLIN SODIUM MONOHYDRATE']='4HZT2V9KX0'
    names['CHLORAMINE-T TRIHYDRATE']='4IU6VSV0EI'
    names['OXYTOCIN ACETATE']='4NR672T8NL'
    names['STANOZOLOL']='4R1VB9P8V3'
    names['ESTRADIOL']='4TI98Z838E'
    names['OXYTETRACYCLINE HYDROCHLORIDE']='4U7K4N52ZM'
    names['MEPIVACAINE HYDROCHLORIDE']='4VFX2L7EM5'
    names['MELENGESTROL ACETATE']='4W5HDS3936'
    names['MAROPITANT']='4XE2T9H4DH'
    names['(S) - METHOPRENE']='4YIQ0A94UR'
    names['DECOQUINATE']='534I52PVWH'
    names['DOMPERIDONE']='5587267Z69'
    names['PERGOLIDE MESYLATE']='55B9HQY616'
    names['ISOFLUPREDONE ACETATE']='55P9TUL75S'
    names['DIRLOTAPIDE']='578H0RMP25'
    names['CHLORPHENESIN CARBAMATE']='57U5YI11WP'
    names['NAPROXEN']='57Y76R9ATQ'
    names['CHLORHEXIDINE ACETATE']='5908ZUF22Y'
    names['LINDANE']='59NEE7PCAB'
    names['EFROTOMYCIN']='5BPJ82Q45X'
    names['SULFAMETHAZINE BISULFATE']='5J847L84W0'
    names['FURAZOLIDONE']='5J9CPU3RE0'
    names['TETRACAINE HYDROCHLORIDE']='5NF5D4OPCI'
    names['TYLOSIN TARTRATE']='5P4625C51T'
    names['2-MERCAPTOBENZOTHIAZOLE']='5RLR54Z22K'
    names['DEXTROSE']='IY9XDZ35W2'
    names['MADURAMICIN AMMONIUM']='5U912U22T2'
    names['MORANTEL TARTRATE']='5WF7E9QC3F'
    names['OXYMORPHONE HYDROCHLORIDE']='5Y2EI94NBC'
    names['DIPERODON HYDROCHLORIDE']='5YZ5R8I73Y'
    names['FENBENDAZOLE']='621BVT9M36'
    names['BISMUTH SUBSALICYLATE']='62TEY51RR1'
    names['ERYTHROMYCIN']='63937KV33D'
    names['PRAZIQUANTEL']='6490C9U457'
    names['CLOXACILLIN SODIUM']='65LCB00B4Y'
    names['ORBIFLOXACIN']='660932TPY6'
    names['CHLORAMPHENICOL']='66974FR9Q1'
    names['DESLORELIN ACETATE']='679007NR5C'
    names['DEXMEDETOMIDINE']='67VB76HONO'
    names['CEFTIOFUR HYDROCHLORIDE']='6822A07436'
    names['CUPRIC GLYCINATE']='68VAV8QID7'
    names['PYRIDOXINE HYDROCHLORIDE']='68Y4CF58BV'
    names['METHENAMINE MANDELATE']='695N30CINR'
    names['CHLOROQUINE PHOSPHATE']='6E17K3343P'
    names['FLUPROSTENOL SODIUM']='6H4ZY4O7NA'
    names['PRADOFLOXACIN']='6O0T5E048I'
    names['POSACONAZOLE']='6TK1G07BHZ'
    names['COPPER DISODIUM EDETATE']='6V475AX06U'
    names['SELEGILINE HYDROCHLORIDE']='6W731X367Q'
    names['SULFISOXAZOLE']='740T4C525W'
    names['EPRINOMECTIN']='75KP30FD8O'
    names['HYDROXYZINE HYDROCHLORIDE']='76755771U3'
    names['ZERANOL']='76LO2L2V39'
    names['CHLOROTHIAZIDE']='77W477J15H'
    names['IODOCHLORHYDROXYQUIN']='7BHQ856EJ5'
    names['BETAMETHASONE SODIUM PHOSPHATE']='7BK02SCL3W'
    names['ATROPINE']='7C0697DR9I'
    names['AMPICILLIN ANHYDROUS']='7C782967RD'
    names['MEBEZONIUM IODIDE']='7GVF119EM8'
    names['CARFENTANIL CITRATE']='7LG286J8GV'
    names['FUROSEMIDE']='7LXU5N7ZO5'
    names['PIPERAZINE MONOHYDROCHLORIDE']='7N36JHA4P6'
    names['NIFURPIRINOL']='7O5A98XY8U'
    names['SQUALENE']='7QWM220FJH'
    names['DEXAMETHASONE']='7S5I7G3JQL'
    names['DICHLORVOS']='7U370BPS14'
    names['PROTOKYLOL HYDROCHLORIDE']='7U7O8Q48IO'
    names['SODIUM SULFAMETHAZINE']='7Z13P9Q95C'
    names['AMOXICILLIN TRIHYDRATE']='804826J2HU'
    names['PYRANTEL PAMOATE']='81BK194Z5M'
    names['MEBENDAZOLE']='81G6I5V05I'
    names['PREDNISOLONE SODIUM SUCCINATE']='8223RR9DWF'
    names['BETAMETHASONE DIPROPIONATE']='826Y60901U'
    names['CYCLOSPORINE']='83HN0GTJ6D'
    names['CEFTIOFUR CRYSTALLINE FREE ACID']='83JL932I1C'
    names['FOMEPIZOLE']='83LCM6L2BY'
    names['SULFADIAZINE SODIUM']='84CS1P306F'
    names['LUBABEGRON']='8501207BZX'
    names['HEXETIDINE']='852A84Y8LS'
    names['IPRONIDAZOLE HYDROCHLORIDE']='87813M60WF'
    names['SULFAETHOXYPYRIDAZINE']='880RIW1DED'
    names['CLOPROSTENOL SODIUM']='886SAV9675'
    names['RONNEL']='89RAG7SB3B'
    names['PREDNISOLONE ACETATE']='8B2807733D'
    names['SEMDURAMICIN SODIUM']='8B50X0IVEC'
    names['ETORPHINE HYDROCHLORIDE']='8CBE01N748'
    names['PHENYLPROPANOLAMINE HYDROCHLORIDE']='8D5I63UE1Q'
    names['CLOPIDOL']='8J763HFF5N'
    names['NICLOSAMIDE']='8KK8CQ2K8G'
    names['DITHIAZANINE IODIDE']='8OEC3RA07X'
    names['CARBARSONE']='8PK70TXE1T'
    names['PIRLIMYCIN HYDROCHLORIDE']='8S09O559AQ'
    names['ROBENIDINE HYDROCHLORIDE']='8STT15Y392'
    names['PIPERAZINE PHOSPHATE']='8TIF7T48FP'
    names['CALCIUM DISODIUM EDETATE']='8U5D034955'
    names['APRAMYCIN SULFATE']='8UYL6NAZ3Q'
    names['MARBOFLOXACIN']='8X09WU898T'
    names['FLUNIXIN MEGLUMINE']='8Y3JK0JW3U'
    names['MONENSIN']='906O0YJ6ZP'
    names['CEPHAPIRIN BENZATHINE']='90G868409O'
    names['KETOPROFEN']='90Y4QC304K'
    names['ENFLURANE']='91I69L5AY5'
    names['NEQUINATE']='91ZE013933'
    names['NITROFURANTOIN']='927AH8112L'
    names['SALINOMYCIN SODIUM']='92UOD3BMEK'
    names['FOLIC ACID']='935E97BOY8'
    names['SPECTINOMYCIN']='93AKI1U6QF'
    names['AMPROLIUM']='95CO6N199Q'
    names['DETOMIDINE HYDROCHLORIDE']='95K4LKB6QE'
    names['HETACILLIN POTASSIUM']='95PFX5932Y'
    names['PROCAINE HYDROCHLORIDE']='95URV01IDQ'
    names['LENPERONE HYDROCHLORIDE']='96Q0TL6O3G'
    names['TRICAINE METHANESULFONATE']='971ZM8IPK1'
    names['NEOSTIGMINE METHYLSULFATE']='98IMH7M386'
    names['ROMIFIDINE HYDROCHLORIDE']='98LQ6RS0TA'
    names['LIDOCAINE']='98PI200987'
    names['OCLACITINIB']='99GS5XTB51'
    names['TILETAMINE HYDROCHLORIDE']='99TAQ2QWJI'
    names['MELARSOMINE DIHYDROCHLORIDE']='9CVA716Q71'
    names['NITROMIDE']='9DUJ3CMK8S'
    names['TRIFLUPROMAZINE HYDROCHLORIDE']='9E75N4A5HM'
    names['NALORPHINE HYDROCHLORIDE']='9FPE56Z2TW'
    names['BETAMETHASONE VALERATE']='9IFA5XM7R2'
    names['COPPER NAPHTHENATE']='9J2IBN2H70'
    names['LEVOTHYROXINE SODIUM']='9J765S329G'
    names['FLORFENICOL']='9J97307Y1H'
    names['AMINOPROPAZINE FUMARATE']='R520B454OA'
    names['ENALAPRIL MALEATE']='9O25354EPJ'
    names['MIBOLERONE']='9OGY4BOR8D'
    names['PREDNISOLONE']='9PHQ9Y1OLM'
    names['TYLVALOSIN']='9T02S42WQO'
    names['CHLORPROMAZINE HYDROCHLORIDE']='9WP59609J6'
    names['MIRTAZAPINE']='A051Q2099Q'
    names['SELAMECTIN']='A2669OWX9N'
    names['ETHYLISOBUTRAZINE HYDROCHLORIDE']='A7002E7T2Z'
    names['LEAD ARSENATE']='A9AI2R9EWN'
    names['CLOXACILLIN BENZATHINE']='AC79L7PV2G'
    names['DEXAMETHASONE SODIUM PHOSPHATE']='AI9376Y64P'
    names['TYLVALOSIN TARTRATE']='AL5667FY0W'
    names['TRIMETHOPRIM']='AN164J8Y0X'
    names['ZOALENE']='AOX68RY4TV'
    names['AKLOMIDE']='B0E341RA20'
    names['HYDROGEN PEROXIDE']='BBX060AN9V'
    names['ALFAXALONE']='BD07M97B2A'
    names['MEDETOMIDINE HYDROCHLORIDE']='BH210P244U'
    names['TILUDRONATE DISODIUM']='BH6M93CIA0'
    names['FENTHION']='BL0L45OVKT'
    names['LINCOMYCIN']='BOD072YW0F'
    names['DIPIPERAZINE SULFATE']='C8493J9B36'
    names['PIPERAZINE SULFATE']='C8493J9B36'
    names['DINOPROST TROMETHAMINE']='CT6BBQ5A68'
    names['STREPTOMYCIN SULFATE']='CW25IKJ202'
    names['ISOFLURANE']='CYS9AKD70P'
    names['MUPIROCIN']='D0GX863OA5'
    names['TRICHLORFON']='DBF2DG4G2K'
    names['CEFOVECIN SODIUM']='DL8Q24959P'
    names['LEVAMISOLE HYDROCHLORIDE']='DL9055K809'
    names['SAROLANER']='DM113FTW7F'
    names['NARASIN']='DZY9VU539P'
    names['ISOPROPAMIDE IODIDE']='E0KNA372SZ'
    names['TIAMULIN']='E38WZ4U54R'
    names['GLYCOBIARSOL']='E3U8347QWJ'
    names['CHLORHEXIDINE HYDROCHLORIDE']='E64XL9U38K'
    names['TOLAZOLINE HYDROCHLORIDE']='E669Z6S1JG'
    names['POTASSIUM CITRATE']='EE90ONI6FF'
    names['CLORSULON']='EG1ZDO6LRD'
    names['ZILPATEROL HYDROCHLORIDE']='EX8IEP25JU'
    names['ALBENDAZOLE']='F4216019LN'
    names['TRIAMCINOLONE ACETONIDE']='F446C597KA'
    names['ETHOPABATE']='F4X3L6068O'
    names['NALOXONE HYDROCHLORIDE']='F850569PQR'
    names['TETRACYCLINE']='F8VB5M810T'
    names['ESTRIOL']='FB33469R8E'
    names['CARPROFEN']='FFL0D546HO'
    names['LEVAMISOLE PHOSPHATE']='FIG89N8AZY'
    names['TRIPELENNAMINE HYDROCHLORIDE']='FWV8GJ56ZN'
    names['CLOTRIMAZOLE']='G07GZ97H65'
    names['TERBINAFINE']='G7RIW8S0XP'
    names['TICARCILLIN DISODIUM']='G8TVV6DSYG'
    names['LIOTHYRONINE SODIUM']='GCA9VV7D2N'
    names['PHENYLBUTAZONE']='GN5P7K3T8S'
    names['CLENBUTEROL HYDROCHLORIDE']='GOR5747GWU'
    names['PHENOTHIAZINE']='GS9EX7QNU6'
    names['HEXAMETHYLTETRACOSANE']='GW89575KF9'
    names['ROXARSONE']='H5GU9YQL7L'
    names['LOTILANER']='HEH4938D7K'
    names['SODIUM SELENITE']='HIW548RQ3W'
    names['CHLOROBUTANOL']='HM4YQM8WRC'
    names['THIOSTREPTON']='HR4S203Y18'
    names['LUPROSTIOL']='HWR60H5GZB'
    names['SPECTINOMYCIN DIHYDROCHLORIDE PENTAHYDRATE']='HWT06H303Z'
    names['SPECTINOMYCIN HYDROCHLORIDE PENTAHYDRATE']='HWT06H303Z'
    names['AMPICILLIN TRIHYDRATE']='HXQ6A1N7R6'
    names['PROCHLORPERAZINE DIMALEATE']='I1T8O1JTL6'
    names['PROCHLORPERAZINE MALEATE']='I1T8O1JTL6'
    names['SARAFLOXACIN HYDROCHLORIDE']='I36JP4Q9DF'
    names['PENTOBARBITAL']='I4744080IR'
    names['ERYTHROMYCIN PHOSPHATE']='I8T8KU14X7'
    names['FLUOXETINE HYDROCHLORIDE']='I9W7N6B1KJ'
    names['TIAMULIN HYDROGEN FUMARATE']='ION1Q02ZCX'
    names['PREDNISOLONE SODIUM PHOSPHATE']='IV021NXA9J'
    names['TICARBODINE']='J4CLF34O60'
    names['KANAMYCIN SULFATE']='J80EX28SMQ'
    names['GRAPIPRANT']='J9F5ZPH7NB'
    names['AMPICILLIN SODIUM']='JFN36L5S8K'
    names['MELATONIN']='JL5DK93RCL'
    names['DISOPHENOL SODIUM']='JOS947MY69'
    names['NITARSONE']='JP2EN8WORU'
    names['PONAZURIL']='JPW84AS66U'
    names['CORTICOTROPIN']='K0U68Q2TXA'
    names['DICLAZURIL']='K110K1B1VE'
    names['CEPHALONIUM']='K2P920217W'
    names['NITROUS OXIDE']='K50XQU1029'
    names['DIMETRIDAZOLE']='K59P7XNB8X'
    names['METOSERPATE HYDROCHLORIDE']='KBO7409339'
    names['OMEPRAZOLE']='KG60484QX9'
    names['DORAMECTIN']='KGD7A54H5P'
    names['PIPERACETAZINE']='KL6248WNW4'
    names['SODIUM SULFACHLOROPYRAZINE MONOHYDRATE']='KPM50228FR'
    names['ARSENAMIDE SODIUM']='KW75J7708X'
    names['COUMAPHOS']='L08SZ5Z5JC'
    names['TRILOSTANE']='L0FPV48Q5R'
    names['DIBUTYLTIN DILAURATE']='L4061GMT90'
    names['FLUMETHASONE']='LR3CD8SX89'
    names['PIPERONYL BUTOXIDE']='LWK91TU9AH'
    names['EPINEPHRINE ACETATE']='M1NJX34RVJ'
    names['CARBADOX']='M2X04R2E2Y'
    names['RABACFOSADINE']='M39BO43J9W'
    names['ORMETOPRIM']='M3EFS94984'
    names['BISMUTH SUBCARBONATE']='M41L2IN55T'
    names['LINCOMYCIN HYDROCHLORIDE']='M6T05Z2B68'
    names['LINCOMYCIN HYDROCHLORIDE MONOHYDRATE']='M6T05Z2B68'
    names['BUQUINOLATE']='MFL71K7PU4'
    names['MAGNESIUM SULFATE']='DE08037SAB'
    names['SODIUM EDETATE']='MP1J8420LU'
    names['MEDETOMIDINE']='MR15E85MQM'
    names['MOMETASONE FUROATE MONOHYDRATE']='MTW0WEG809'
    names['FENTANYL CITRATE']='MUN5LYG46H'
    names['SODIUM SULFACHLORPYRIDAZINE']='N1LMA4960O'
    names['THIABENDAZOLE']='N1Q45E87DT'
    names['AMIKACIN SULFATE']='N6M33094FD'
    names['NITROGEN']='N762921K75'
    names['YOHIMBINE HYDROCHLORIDE']='NB2E1YP49F'
    names['XYLAZINE HYDROCHLORIDE']='NGC3S0882S'
    names['MOXIDECTIN']='NGU5H31YO9'
    names['CEFTIOFUR SODIUM']='NHI34IS56E'
    names['PENTOBARBITAL SODIUM']='NJJ0475N0S'
    names['CHLORTETRACYCLINE CALCIUM COMPLEX']='NR4B2SX17S'
    names['TETRACYCLINE PHOSPHATE']='NZ662XY5PP'
    names['TIOXIDAZOLE']='NZW046NI85'
    names['PROSTALENE']='O02SWY8981'
    names['KETAMINE HYDROCHLORIDE']='O18YUO0I83'
    names['CHLORTETRACYCLINE HYDROCHLORIDE']='O1GX33ON8R'
    names['SALICYLIC ACID']='O414PZ4LPZ'
    names['DROPERIDOL']='O9U0F09D5X'
    names['CEPHALEXIN']='OBN7UDS42Y'
    names['ESTRADIOL VALERATE']='OKG364O896'
    names['OXFENDAZOLE']='OMP2H17F9E'
    names['DIETHYLCARBAMAZINE CITRATE']='OS1Z389K8S'
    names['STYRYLPYRIDINIUM CHLORIDE']='OW4S2EQU3J'
    names['DOXAPRAM HYDROCHLORIDE']='P5RU6UOQ5Y'
    names['TETRACYCLINE HYDROCHLORIDE']='P6R62377KV'
    names['SULFACHLORPYRIDAZINE']='P78D9P90C0'
    names['OLEANDOMYCIN']='P8ZQ646136'
    names['BUTACAINE SULFATE']='PAU39W3CVB'
    names['PROCHLORPERAZINE EDISYLATE']='PG20W5VQZS'
    names['ORGOTEIN']='PKE82W49V1'
    names['HALOFUGINONE HYDROBROMIDE']='PTC2969MV1'
    names['PIPEROCAINE HYDROCHLORIDE']='Q2RH0XR1MB'
    names['DIAZEPAM']='Q3JTX2Q7TU'
    names['CLAVULANATE POTASSIUM']='Q42OMW3AT8'
    names['TRICHLORMETHIAZIDE']='Q58C92TUN0'
    names['CUPRIMYXIN']='Q728680892'
    names['NOVOBIOCIN SODIUM']='Q9S9NQ5YIY'
    names['BUTAMISOLE HYDROCHLORIDE']='QGM18599H5'
    names['SULFANITRAN']='QT35T5T35Q'
    names['DICLOFENAC SODIUM']='QTG126297Q'
    names['ACETYLSALICYLIC ACID']='R16CO5Y76E'
    names['PYRILAMINE MALEATE']='R35D29L3ZA'
    names['CEPHALOTHIN']='R72LW146E6'
    names['CARNIDAZOLE']='RH5KI819JG'
    names['THIALBARBITONE SODIUM']='RHK739S84F'
    names['PENICILLIN G BENZATHINE']='RIT82F58GK'
    names['MAFENIDE ACETATE']='RQ6LP6Z0WY'
    names['TRENBOLONE ACETATE']='RUD5Y4SV0S'
    names['ZILPATEROL']='S384A1Y12J'
    names['FEBANTEL']='S75C401OS1'
    names['TILDIPIROSIN']='S795AT66JB'
    names['OXYGEN']='S88TT14065'
    names['PYRANTEL TARTRATE']='SC82VF0480'
    names['TILMICOSIN PHOSPHATE']='SMH7U1S683'
    names['NITAZOXANIDE']='SOA12P041N'
    names['DICHLOROPHENE']='T1J0JOU64O'
    names['CLINDAMYCIN HYDROCHLORIDE']='T20OQ1YN1W'
    names['THIAMYLAL SODIUM']='T4L2P3KH7K'
    names['DIHYDROSTREPTOMYCIN SULFATE']='T7D4876IUE'
    names['HALOXON']='T8KXA37068'
    names['PHTHALOFYNE']='TA9XO4D05J'
    names['GLYCINE']='TE7660XO1C'
    names['BETAMETHASONE ACETATE']='TI05AO53L7'
    names['MEGESTROL ACETATE']='TJ2M0FR8ES'
    names['VITAMIN B2']='TLM2976OFR'
    names['THENIUM CLOSYLATE']='TU308VI4JY'
    names['TEPOXALIN']='TZ4OX61974'
    names['PROPIOPROMAZINE HYDROCHLORIDE']='U0BND6SD2I'
    names['PROMAZINE HYDROCHLORIDE']='U16EOR79U4'
    names['TELMISARTAN']='U5SYW473RQ'
    names['ATTAPULGITE']='U6V729APAM'
    names['ZINC GLUCONATE']='U6WSN5SQ1Z'
    names['PROPARACAINE HYDROCHLORIDE']='U96OL57GOY'
    names['ARSANILIC ACID']='UDX9AKS7GM'
    names['FENTANYL']='UF599785JZ'
    names['HALOTHANE']='UQT9G45D1P'
    names['SULFAMERAZINE']='UR1SAB295F'
    names['LIDOCAINE HYDROCHLORIDE']='V13007Z41A'
    names['DIATRIZOATE SODIUM']='V5403H8VG7'
    names['PIPERAZINE ADIPATE']='V7P5P122LB'
    names['CRUFOMATE']='V82Q65924L'
    names['GLYCOPYRROLATE']='V92SO9WP2I'
    names['DOXYLAMINE SUCCINATE']='V9BI9B5YI2'
    names['PREDNISONE']='VB0R961HZT'
    names['MELOXICAM']='VG2QF83CGL'
    names['PENICILLIN G POTASSIUM']='VL775ZTH4C'
    names['PHOSMET']='VN04LI540Y'
    names['MICONAZOLE NITRATE']='VW4H1CYW1K'
    names['DERACOXIB']='VX29JB5XWV'
    names['LASALOCID SODIUM']='W2S5C71Y3G'
    names['DIPHEMANIL METHYLSULFATE']='W2ZG23MGYI'
    names['SILVER SULFADIAZINE']='W46JY43EJR'
    names['LASALOCID']='W7V2ZZ2FWB'
    names['DIPRENORPHINE HYDROCHLORIDE']='WBS7IEP4SN'
    names['CHLORTETRACYCLINE']='WCK1KIQ23Q'
    names['HYDROCORTISONE']='WI4X0X7BPJ'
    names['TESTOSTERONE PROPIONATE']='WI93Z9138A'
    names['SULFAQUINOXALINE']='WNW8115TM9'
    names['FLURALANER']='WSH8393RM5'
    names['OXYTETRACYCLINE']='X20I9EN955'
    names['OXYTETRACYCLINE DIHYDRATE']='X20I9EN955'
    names['METHYLPREDNISOLONE']='X4W7ZR7023'
    names['VITAMIN B1']='X66NSO3N35'
    names['FENPROSTALENE']='X8I39OJF4P'
    names['NITROFURAZONE']='X8XI70B5Z6'
    names['SECOBARBITAL SODIUM']='XBP604F6UM'
    names['DIFLOXACIN HYDROCHLORIDE']='XJ0260HJ0O'
    names['TILMICOSIN']='XL4103X2E3'
    names['SULFABROMOMETHAZINE SODIUM']='Y200FZX73L'
    names['STREPTOMYCIN']='Y45QSO73OB'
    names['FIROCOXIB']='Y6V2W4S4WT'
    names['ERYTHROMYCIN THIOCYANATE']='Y7A95YRI88'
    names['SULFATHIAZOLE']='Y7FKS2XWQH'
    names['BUNAMIDINE HYDROCHLORIDE']='Y80LB0Q7CB'
    names['BUPIVACAINE']='Y8335394RO'
    names['TYLOSIN']='YEF4JXN031'
    names['TRIFLUOMEPRAZINE MALEATE']='YF6LF27282'
    names['PROPOFOL']='YI7VU623SF'
    names['DIMETHYL SULFOXIDE']='YOW8V9698H'
    names['SPECTINOMYCIN SULFATE TETRAHYDRATE']='YS91P54918'
    names['EMODEPSIDE']='YZ647Y5GC9'
    names['PYRIMETHAMINE']='Z3614QOX8W'
    names['ROBENACOXIB']='Z588009C7C'
    names['NALTREXONE HYDROCHLORIDE']='Z6375YW9SF'
    names['SELENIUM DISULFIDE']='Z69D9E381Q'
    names['DIBUCAINE HYDROCHLORIDE']='Z97702A5DG'
    names['GAMITHROMYCIN']='ZE856183S0'
    names['N-BUTYL CHLORIDE']='ZP7R667SGD'
    names['BOLDENONE UNDECYLENATE']='ZS6D2ITA30'
    names['IMIDOCARB DIPROPIONATE']='ZSM1M03SHC'
    names['AMMONIUM CHLORIDE (NH4CL)']='01Q9PC255D'
    names['HLAL RDNA CONSTRUCT IN SBC LAL-C CHICKENS']='MG6AU4C2HB'
    names['OXYTETRACYCLINE (MONOALKYL TRIMETHYL AMMONIUM SALT)']='M1E0I5GQNH'
    names['PREGELATINIZED STARCH']='O8232NY3SJ'
    names['FLUMETHASONE ACETATE']='HB84ATQ00X'
    names['LEVAMISOLE RESINATE']='2880D3468G'
    names['PIPERAZINE-CARBON DISULFIDE COMPLEX']='6Z6020Q81C'
    names['ARSANILATE SODIUM']='UC2409302Q'
    names['BC6 RECOMBINANT DEOXYRIBONUCLEIC ACID CONSTRUCT']='AWV6I5L6H2'
    names['MEDICAL AIR, USP']='K21NZZ5Y0B'
    names['BOVINE SOMATOTROPIN (SOMETRIBOVE ZINC)']='PBK5EQG5CQ'
    names['PORCINE PITUITARY-DERIVED FOLLICLE STIMULATING HORMONE']='8FYM5179QJ'
    names['DINITRODIPHENYLSULFONYLETHYLENEDIAMINE']='P4KIO8KEG5'
    names['GONADOTROPIN RELEASING FACTOR  DIPHTHERIA TOXOID CONJUGATE']='1DBT5N7G0X'
    names['OPAFP-GHC2 RDNA CONSTRUCT']='EC8TZL340I'
    names['NITROGEN, NF']='N762921K75'
    names['CYCLOSPORINE ORAL SOLUTION, USP MODIFIED']='83HN0GTJ6D'
    names['PITUITARY LUTEINIZING HORMONE']='8XA4VN1LH4'
    names['THYROID STIMULATING HORMONE']='02KSI6Z9AK'
    names['FELIMAZOLE']='554Z48XN5E'

    return names
    
defaultNames = defaultNames()
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

if __name__=="__main__":
    
    scrapeNADAs()
    
    fp = open(newnadasjson, "r")
    lines = fp.readlines()
    fp.close()

    apps = dict()
    for line in lines[1:-1]:
        if line[-2:] == ",\n":
            line = line[:-2]
        obj = json.loads(line, encoding='utf8')
        app = str(obj['applicationNumber'])
        app = app[:-3] + "-" + app[-3:]
        while len(app) < 7:
            app = "0" + app
        tradeName = ""
        routes = []
        rx = ""
        ingredients = []
        status = ""
        sponsor = ""
        apptype = ""
        if obj.has_key('proprietaryPreviewBean'):
            tradeName = obj['proprietaryPreviewBean'][0]['proprietaryName']
            routes = obj['proprietaryPreviewBean'][0]['routes']
            rx = obj['proprietaryPreviewBean'][0]['statusDescription']
        if obj.has_key('ingredientsPreviewBean'):
            for item in obj['ingredientsPreviewBean']:
                ingredients.append(item['activeIngredientName'])
        else:
            sys.stderr.write("NADA application has no ingredients: "+app+"\n")
        if obj.has_key('applicationStatus'):
            status = obj['applicationStatus']
        if obj.has_key('sponsorPreviewBean'):
            sponsor = obj['sponsorPreviewBean']['sponsorName']
        if obj.has_key('applicationType'):
            apptype = obj['applicationType']
        newobj = dict()
        newobj['AppNo'] = app
        newobj['Trade Name'] = tradeName
        newobj['Routes'] = routes
        newobj['Rx'] = rx
        newobj['Ingredients'] = ingredients
        newobj['Status'] = status
        newobj['Sponsor'] = sponsor
        newobj['AppType'] = apptype
        newobj['Reference'] = 'https://animaldrugsatfda.fda.gov/adafda/views/#/home/previewsearch/'+app
        apps[app] = newobj

    faradRoutes = faradRoutes()
    fp = open(faradProds, "r")
    lines = fp.readlines()
    fp.close()

    for line in lines[1:-1]:
        if line[-2:] == ",\n":
            line = line[:-2]
        obj = json.loads(line, encoding='utf8')
        app = obj['NADA/ANADA']
        tradeName = obj['Trade Name']
        routes = []
        for item in obj['Route'].split("|"):
            if faradRoutes.has_key(item):
                routes.append(faradRoutes[item].split("; ")[0])
            else:
                routes.append(item)
        rx = obj['Rx'].upper()
        ingredients = obj['Active Ingredient'].split("|")
        status = obj['Market Status']
        sponsor = ""
        if obj.has_key('Manufacturer'):
            sponsor = obj['Manufacturer']
        apptype = "N"
        if app[0:3] == "200":
            apptype = "A"
        
        if apps.has_key(app):
            newobj = apps[app]
        else:
            newobj = dict()
            newobj['AppNo'] = app
            newobj['Trade Name'] = tradeName
            newobj['Routes'] = routes
            newobj['Rx'] = rx
            newobj['Ingredients'] = ingredients
            newobj['Status'] = status
            newobj['Sponsor'] = sponsor
            newobj['AppType'] = apptype
            newobj['Reference'] = 'http://www.farad.org/vetgram/ProductInfo.asp?byNada='+app
            apps[app] = newobj


    ingreds = dict()
    for app in apps.keys():
        if len(apps[app]['Ingredients']) == 0 and len(apps[app]['Trade Name']) > 0:
            apps[app]['Ingredients'] = [apps[app]['Trade Name']]
        for item in apps[app]['Ingredients']:
            if not ingreds.has_key(item.upper()):
                ingreds[item.upper()] = app
            elif app < ingreds[item.upper()] or (apps[ingreds[item.upper()]]['Status'] != "Approved" and apps[app]['Status'] == 'Approved'):
                ingreds[item.upper()] = app
        #data = json.dumps(apps[app], ensure_ascii=False, encoding='utf8')
        #print data

    out = ["AppNo", "Rx", "Status", "AppType", "Sponsor", "Trade Name", "Reference", "Routes", "Ingredients"]
    outFile = io.open(FDAanimalDrugstxt, 'w', encoding='utf8')
    outFile.write(unicode("NADA_ID\tUNII\tIngredient\tAppNo\tRx\tStatus\tAppType\tSponsor\tTrade Name\tReference\tRoutes\tIngredients\n"))
    for ingred in ingreds:
        app = ingreds[ingred]
        outline = u""
        outline = outline + ingred
        for item in out:
            if item == "Routes" or item == "Ingredients":
                outline = outline + "\t" + "|".join(apps[app][item])
            else:
                outline = outline + "\t"
                if apps[app][item] != None:
                    outline = outline + apps[app][item]
        unii = resolveName(ingred)
        outline = unii + apps[app]["AppNo"] + "\t" + unii + "\t" + outline
        outFile.write(outline)
        outFile.write(unicode("\n"))
        #data = json.dumps(apps[app], ensure_ascii=False, encoding='utf8')
        #print ingred, data
    outFile.close()
