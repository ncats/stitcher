from metapub import PubMedFetcher

import os
os.environ['NCBI_API_KEY'] = '4052151f1e87f75626c8159aaf42d6e72708'


fetch = PubMedFetcher()
chunk_size = 100000
for start in range(30000000, 40000000, chunk_size):
    list = fetch.pmids_for_query(f"({start}:{start + chunk_size - 1}[uid]) AND (clinical trial[Publication Type] NOT review[Publication Type])", retmax=10000)
    print(f"({start}:{start + chunk_size - 1}): {len(list)}")
