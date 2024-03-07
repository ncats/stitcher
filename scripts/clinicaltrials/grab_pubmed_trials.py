#!/usr/bin/python3

import xml.etree.ElementTree as ET
import gzip
import requests
import os
import time

from ct_utils import xmlfind

esearch_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&usehistory=y"
efetch_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&retmax=10000"

def clin_trial_pub_query(min_id = None, max_id = None):
    if not min_id and not max_id:
        return "(clinical+trial[Publication+Type])+NOT+(review[Publication+Type])"
    return f"({min_id}:{max_id}[uid]) AND (clinical trial[Publication Type] NOT review[Publication Type])"

def get_max_pmid_for_ct_pubs():
    full_ct_query = f"{esearch_base}&term={clin_trial_pub_query()}"
    response = requests.get(full_ct_query)
    root = ET.fromstring(response.content)
    return int(xmlfind(root, './IdList/Id'))

def get_esearch_meta_keys(min_id, max_id):
    page_ct_query = f"{esearch_base}&term={clin_trial_pub_query(min_id, max_id)}"
    response = requests.get(page_ct_query)
    root = ET.fromstring(response.content)
    count = int(xmlfind(root, './Count'))
    query_key = xmlfind(root, './QueryKey')
    webenv = xmlfind(root, './WebEnv')
    return webenv, query_key, count

def grabPubMedTrials():
    max_pmid = get_max_pmid_for_ct_pubs()
    print(f"fetching articles up to id: {max_pmid}")
    if not os.path.exists('../stitcher-inputs/temp/pubmed'):
        os.mkdir('../stitcher-inputs/temp/pubmed')
    chunk_size = 100000
    for start in range(0, max_pmid, chunk_size):
        try:
            search_range = f"{start}-{start + chunk_size - 1}"
            if not os.path.exists(f"../stitcher-inputs/temp/pubmed/pmds{search_range}.xml.gz"):
                webenv, query_key, count = get_esearch_meta_keys(start, start + chunk_size - 1)
                print(f"found {count} clinical trial publications in {search_range}")
                if (count > 9998):
                    raise Exception("Your chunk size is too big, efetch won't return more than 9998 at a time")
                url = efetch_base+"&WebEnv="+webenv+"&query_key="+query_key
                print(url)
                r2 = requests.get(url)
                fp = gzip.open(f"../stitcher-inputs/temp/pubmed/pmds{search_range}.xml.gz", "wb")
                fp.write(r2.content)
                fp.close()
                time.sleep(3)
        except Exception as e:
            print(e)
            raise e

    with open(f"../stitcher-inputs/temp/pubmed/clinical_trials_downloads_are_complete.txt", 'wt') as f:
        f.write('done')
        f.close()


if __name__=="__main__":
    grabPubMedTrials()
