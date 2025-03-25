import os
import io
import sys
import cookielib
import urllib2
import json
import time

cookies = cookielib.CookieJar()

opener = urllib2.build_opener(
    urllib2.HTTPRedirectHandler(),
    urllib2.HTTPHandler(debuglevel=0),
    urllib2.HTTPSHandler(debuglevel=0),
    urllib2.HTTPCookieProcessor(cookies))
opener.addheaders = [
    ('User-agent', ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'))
]

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

def resolveNameTripod(name):
    unii = ''

    request = "https://drugs.ncats.io/api/v1/substances/search?q=root_names_name:\"^"+urllib2.quote(name)+"$\""
    response = "{\"total\":0}"
    try:
        response = urllib2.urlopen(request).read()
    except:
        response
        time.sleep(5)
    r = json.loads(response)
    if int(r['total']) > 0:
        for i in range(0, int(r['total'])):
            if r['content'][i].has_key('_approvalIDDisplay'):
                if unii == '':
                    unii = r['content'][i]['_approvalIDDisplay']
                elif unii != r['content'][i]['_approvalIDDisplay']:
                    #raise ValueError("conflicting UNIIs returned:"+name+"::"+request)
                    print "conflicting UNIIs returned:"+name+"::"+request
                    return ""
            elif r['content'][i].has_key('approvalID'):
                if unii == '':
                    unii = r['content'][i]['approvalID']
                elif unii != r['content'][i]['approvalID']:
                    #raise ValueError("conflicting UNIIs returned:"+name+"::"+request)
                    print "conflicting UNIIs returned:"+name+"::"+request
                    return ""
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
    names['POLYOXYETHYLENE (23) LAURYL ETHER']='N72LMW566G'
    names['25-HYDROXYVITAMIN D3']='P6YZ13C99Q'
    names['SELEGILINE']='2K1V7GP655'
    names['BETAMETHASONE DIPROPIONATE']='826Y60901U'
    names['SOMETRIBOVE ZINC']='PBK5EQG5CQ'
    names['PENICILLIN G PROCAINE-DIHYDROSTREPTOMYCIN SULFATE']='T7D4876IUE'
    names['PENICILLIN G PROCAINE-DIHYDROSTREPTOMYCIN']='P2I6R8W6UA'
    names['PENICILLIN G PROCAINE-NOVOBIOCIN']='17EC19951N'
    names['OPAFP-GHC2 RECOMBINANT DEOXYRIBONUCLEIC ACID CONSTRUCT']='EC8TZL340I'
    names['HUMAN LYSOSOMAL ACID LIPASE RECOMBINANT DEOXYRIBONUCLEIC ACID CONSTRUCT']='K4YTU42T8G'
    names['CHLORAMINE-T']='4IU6VSV0EI'
    names['13-BUTYLENE GLYCOL']='3XUS85K0RA'
    names['GLUTAMIC ACID']='3KX376GY7L'
    names['MENADIONE NICOTINAMIDE BISULFITE']='I2BE1ZEN8I'
    names['TARTARIC ACID']='W4888I119H'
    names['ALANINE']='OF5P57N2ZX'
    names['CALCIUM PANTOTHENATE']='68ET80C3D'
    names['HISTIDINE']='4QD397987E'
    names['LYSINE']='K3Z4F929H6'
    names['PHENYLALANINE']='47E5O17Y3R'
    names['PROLINE']='9DLQ4CIU6V'
    names['THREONINE']='2ZD004190S'
    names['TRYPTOPHANE']='8DUH1N11BX'
    names['TYROSINE']='42HK56048U'
    names['VALINE']='HG18B9YRS7'
    names['A-TOCOPHEROL ACETATE']='9E8X80D2L0'
    names['CAROTENE']='01YAE03M7J'
    names['GUANIDINOACETIC ACID']='GO52O1A04E'
    names['N-(MERCAPTOMETHYL) PHTHALIMIDE S-(OO-DIMETHYL PHOSPHORODITHIOATE)']='VN04LI540Y'
    names['MANGANOUS OXIDE']='64J2OA7MH3'
    names['MILBEMYCIN OXIME']='0502PUN0GT'
    names['MILBEMYCIN']='0502PUN0GT'
    names['METHIONINE HYDROXY ANALOG']='Z94465H1Y7'
    names['ACID']=''
    names['OXIDE']=''
    names['IN']=''
    names['CREAM']=''
    names['SULFATE']=''
    names['IN SOYBEAN OIL']=''
    names['EGGS']=''
    names['AMMONIATED COTTONSEED MEAL']=''
    names['AMMONIATED RICE HULLS']=''
    names['FERMENTED AMMONIATED CONDENSED WHEY']=''
    names['METHYL ESTERS OF CONJUGATED LINOLEIC ACID (CIS-9 TRANS-11 AND TRANS-10 CIS-12-OCTADECADIENOIC ACIDS)']=''
    names['DIACETYL TARTARIC ACID ESTERS OF MONO- AND DIGLYCERIDES OF EDIBLE FATS OR OILS OR EDIBLE FAT-FORMING FATTY ACIDS']=''
    names['MONOSODIUM PHOSPHATE DERIVATIVES OF MONO- AND DIGLYCERIDES OF EDIBLE FATS OR OILS OR EDIBLE FAT-FORMING FATTY ACIDS']=''
    names['POLYETHYLENE GLYCOL (400) MONO- AND DIOLEATE']=''
    names['POLYOXYETHYLENE GLYCOL (400) MONO- AND DIOLEATES']=''
    names['AMINOGLYCOSIDE 3_-PHOSPHO- TRANSFERASE II']=''
    names[u'AMINOGLYCOSIDE 3\u2032-PHOSPHO- TRANSFERASE II']=''
    names['CALCIUM HEXAMETAPHOSPHATE']=''
    names['METHYL ESTERS OF HIGHER FATTY ACIDS']=''
    names['LIGNIN SULFONATES']=''
    names['SULFADIAZINE/PYRIMETHAMINE SUSPENSION']=''
    names['SODIUM']=''
    names['TOP']=''
    names['YEAST']=''
    names['']=''

    return names
    
defaultNames = defaultNames()
cache = dict()
def resolveName(name):
    try:
        name = name.encode('ascii', 'ignore')
    except:
        return ""
    if name in cache.keys():
        return cache[name]
    unii = ""
    if name in defaultNames.keys():
        unii = defaultNames[name]
    else:
        unii = resolveNameTripod(str(name)+" [GREEN BOOK]")
        if unii == "":
            unii = resolveNameTripod(name)
    if unii == "":
        sys.stderr.write("Can not resolve name to a unii: "+name+"\n")
    cache[name] = unii
    return unii

def findPart(lines, tag, start, end):
    for i in range(start, end):
        if lines[i].find(" <"+tag+">") > -1:
            for j in range(i, end):
                if lines[j].find(" </"+tag+">") > -1:
                    return i+1, j
            return i+1, end
        elif lines[i].find(" </"+tag+">") > -1:
            return start, i
    return start, end

def clipTag(lines, i, tag):
    line = lines[i]
    while line.find("<"+tag) == -1 or line.find("</"+tag+">") == -1:
        line = line.strip()+" "+lines[i+1].strip()
        i = i + 1
    part = line[line.find("<"+tag):line.find("</"+tag+">")]
    part = part[part.find(">")+1:]
    part = part.strip()
    i = part.find("<")
    while i > -1:
        part = part[0:i]+part[part.find(">", i)+1:]
        i = part.find("<")
    return part

if __name__=="__main__":

    #uri = "https://www.gpo.gov/fdsys/bulkdata/CFR/2018/title-21/CFR-2018-title21-vol6.xml"
    #uri = "https://www.govinfo.gov/content/pkg/CFR-2018-title21-vol6/xml/CFR-2018-title21-vol6-part516.xml"
    #uri = "https://www.govinfo.gov/content/pkg/CFR-2018-title21-vol6/xml/CFR-2018-title21-vol6-sec516-1684.xml"
    sections = dict()
    uristem = "https://www.govinfo.gov/content/pkg/CFR-2018-title21-vol6/xml/CFR-2018-title21-vol6-"
    parts = ["part516:E", "part520", "part522", "part524", "part526", "part528", "part529", "part558:B", "part573", "part582:B,C,D,E,F,G,H", "part584"]
    for part in parts:
        subparts = []
        if part.find(":") > -1:
            subparts = part[part.find(":")+1:].split(",")
            part = part[:part.find(":")]
        uri = uristem + part + ".xml"
        print uri
        handle = opener.open(uri)
        response = handle.read().decode('utf-8')
        handle.close()
        lines = response.split("\n")
        subpart = ""
        title = ""
        section = ""
        subject = ""
        start, end = findPart(lines, "CONTENTS", 0, len(lines))
        for i in range(0, end):
            if lines[i].find("<HD") > -1:
                title = clipTag(lines, i,"HD")
                if title[0:8] == "Subpart ":
                    subpart = title[8]
                    title = title[10:]
                else:
                    title = title[0:8]+"-"+title[9:]
            elif lines[i].find("<SECTNO>") > -1:
                section = clipTag(lines, i, "SECTNO")
            elif lines[i].find("<SUBJECT>") > -1:
                subject = clipTag(lines, i, "SUBJECT")
                if len(subparts) == 0 or subpart in subparts:
                    if subject.find("[Reserved]") == -1:
                        uri = uristem + "sec" + section.replace(".", "-") + ".xml"
                        sections[section] = [part, subpart, title, section, subject, uri]
                    
    drugs = dict()
    oops = dict()
    for section in sections.keys():
        entry = sections[section][4].upper()
        entry = entry.replace(".", "")
        entry = entry.replace(", ", " ")
        entry = entry.replace("\"", "")
        sentry = entry.split(" ")
        i = len(sentry)
        while len(sentry) > 0:
            combos = []
            for j in range(0,len(sentry)-i+1):
                combos.append(" ".join(sentry[j:j+i]))
            for combo in combos:
                unii = resolveName(combo)
                if unii != "":
                    if not drugs.has_key(unii):
                        drugs[unii] = []
                    drugs[unii].append([combo, section])
                    print unii, combo, section
                    sys.stdout.flush()
                    for item in combo.split(" "):
                        del sentry[sentry.index(item)]
                    i = len(sentry)+1
                    break
                elif combo in defaultNames.keys():
                    for item in combo.split(" "):
                        del sentry[sentry.index(item)]
                    i = len(sentry)+1
                    break                   
                if i == 1:
                    if not oops.has_key(combo):
                        oops[combo] = []
                    oops[combo].append(entry)
                    del sentry[sentry.index(combo)]
            i = i - 1

    keys = []
    for entry in oops.keys():
        keys.append([len(oops[entry]), entry])
    keys.sort()

    for item in keys:
        try:
            print item[0], item[1].encode('ascii', 'ignore'), str(oops[item[1]]).encode('ascii', 'ignore')
        except:
            print item[0], item[1].encode('ascii', 'ignore')

    i = 1759
    for unii in drugs.keys():
        for entry in drugs[unii]:
            #print entry
            combo = entry[0]
            section = entry[1]
            outline = []
            outline.append(unicode(str(i)))
            outline.append(unicode(unii))
            outline.append(unicode(combo.lower()))
            outline.append(sections[section][2])
            if sections[section][0] in ["part573", "part582", "part584"]:
                outline.append(u"Animal Dietary Supplement")
            else:
                outline.append(u"Animal Drug")
            outline.append(u"21 CFR "+sections[section][3])
            outline.append(unicode(sections[section][5]))
            print u"\t".join(outline).encode('ascii', 'ignore')
            i = i + 1
    
