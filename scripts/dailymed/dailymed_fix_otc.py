import sys
import os
import pandas as pd

path2spl = sys.argv[1]
path2otc = sys.argv[2]

spl = pd.read_csv(path2spl,
                  sep="\t")

otc = pd.read_excel(path2otc,
                    usecols=range(0, 4))

otc = otc.apply(lambda x: x.str.strip())

# spl missing from the otc unii list
spl_miss = spl[~spl.UNII.isin(list(set(otc.UNII)))]

# spl present in the otc unii list
spl_new = spl[spl.UNII.isin(list(set(otc.UNII)))]

# write to tab-separated files
spl.to_csv(os.path.join(os.path.dirname(path2spl),
                        "spl_acti_otc_ORIG.txt"),
           sep="\t",
           index=False,
           encoding="utf-8")

spl_miss.to_csv(os.path.join(os.path.dirname(path2spl),
                             "spl_acti_otc_MISSING.txt"),
                sep="\t",
                index=False,
                encoding="utf-8")

spl_new.to_csv(path2spl,
               sep="\t",
               index=False,
               encoding="utf-8")
