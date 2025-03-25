import json
import os
import sys
import io
import time
import urllib
from http.cookiejar import CookieJar
import pandas

cookies = CookieJar()

opener = urllib.request.build_opener(
    urllib.request.HTTPRedirectHandler(),
    urllib.request.HTTPHandler(debuglevel=0),
    urllib.request.HTTPSHandler(debuglevel=0),
    urllib.request.HTTPCookieProcessor(cookies)
)

opener.addheaders = [
    ('User-agent', (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'))
]


def requestJson(uri):
    try:
        handle = opener.open(uri)
        response = handle.read()
        handle.close()
        obj = json.loads(response)
        return obj
    except Exception as err:
        print(Exception, err)
        sys.stderr.write("failed: " + uri + "\n")
        sys.stderr.flush()
        time.sleep(5)


def scrapeNADAs():
    sec12txt = "../stitcher-inputs/temp/Section12byApplicationNumber.txt"
    sec6txt = "../stitcher-inputs/temp/Section6VoluntaryWithdrawal.txt"

    pandas.read_excel("../stitcher-inputs/temp/Section12byApplicationNumber.xls", sheet_name=0).to_csv(
        path_or_buf=sec12txt, sep='\t', encoding='utf-8', index=False)
    pandas.read_excel("../stitcher-inputs/temp/Section6VoluntaryWithdrawal.xls", sheet_name=0).to_csv(
        path_or_buf=sec6txt, sep='\t', encoding='utf-8', index=False)

    files = [sec12txt, sec6txt]  # "Section12byApplicationNumber.txt", "Section6VoluntaryWithdrawal.txt"]
    apps = dict()
    for filename in files:
        fp = open(filename, "r")
        lines = fp.readlines()
        fp.close()

        for line in lines[1:]:
            sline = line.split("\t")
            appno = sline[0]
            appno = int(appno[0:3] + appno[4:])
            apps[appno] = {}

    jsonFile = io.open("../stitcher-inputs/NADAs.json", 'r')
    for entry in json.loads(jsonFile.read()):
        app = entry['applicationNumber']
        apps[app] = entry
    jsonFile.close()

    uriprefix = 'https://animaldrugsatfda.fda.gov/adafda/app/search/public/application/'
    for i in range(0, 10):
        print('request: ' + uriprefix + str(i))
        obj = requestJson(uriprefix + str(i))
        for item in obj["applicationNumbers"]:
            app = item["applicationNumber"]
            if app not in apps.keys():
                print("Oops!")
                print(app)
                print(item)
                # sys.exit()
            else:
                for entry in item.keys():
                    if not entry in apps[app] or apps[app][entry] != item[entry]:
                        apps[app][entry] = item[entry]

    jsonFile = io.open("../stitcher-inputs/temp/NADAs.json", 'w', encoding='utf8')
    jsonFile.write("[\n")
    for app in apps.keys():
        if apps[app] != {} and 'applicationId' in apps[app] and not 'applicationPatent' in apps[app]:
            # print app
            idnum = apps[app]["applicationId"]
            uri = 'https://animaldrugsatfda.fda.gov/adafda/app/search/public/retrievePreviewBean/' + str(idnum)
            obj = requestJson(uri)
            if obj is not None:
                for item in obj.keys():
                    if item != 'application':
                        apps[app][item] = obj[item]
        else:
            apps[app]["applicationNumber"] = app
        data = json.dumps(apps[app], ensure_ascii=False)
        jsonFile.write(data)
        if app != list(apps.keys())[-1]:
            jsonFile.write(",\n")
    jsonFile.write("]\n")
    jsonFile.close()


if __name__ == "__main__":
    scrapeNADAs()
