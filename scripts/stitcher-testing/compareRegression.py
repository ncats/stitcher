import pandas as pd
import unittest
import argparse

def readHighestStatusDF(filename):
    df = pd.read_excel(filename, sheet_name='highestStatus')
    #df = pd.read_csv(filename, sep="\t")
    df.dropna(subset=['uniis'], inplace=True)
    df.uniis = df.uniis.apply(lambda x: "|".join(sorted(str(x).split('|'))))

    return df

def tieMax(keylist, sets, tieBreakers):
    maxSize = 0
    maxSets = []
    pref = None
    for i in range(len(sets)):
        if len(sets[i]) > maxSize:
            maxSize = len(sets[i])
            maxSets = [keylist[i]]
            if keylist[i] not in tieBreakers:
                pref = keylist[i]
        elif len(sets[i]) == maxSize:
            maxSets.append(keylist[i])
            if keylist[i] not in tieBreakers:
                pref = keylist[i]
    if pref is None:
        return maxSets[0]
    return pref          

# provide two lists of tuples of (index, {set})
# show how the clustering changed as a kind of edit distance
def clusteringDiff(s1, s2):
    sets = dict()
    setitems = []
    s1sets = dict()
    s2sets = dict()

    for i in range(len(s1)):
        missingset = set()
        for item in s1[i][1]:
            for j in range(len(s2)):
                if item in s2[j][1]:
                    aset = set()
                    for elem in s1[i][1] & s2[j][1]:
                        if elem not in aset and elem not in setitems:
                            aset.add(elem)
                            setitems.append(elem)
                    if len(aset) > 0:
                        key = "s"+str(len(sets))
                        sets[key] = aset
                        s1sets.setdefault(i,[]).append(key)
                        s2sets.setdefault(j,[]).append(key)
            if item not in setitems:
                missingset.add(item)
                setitems.append(item)
        if len(missingset) > 0:
            key = "s"+str(len(sets))
            sets[key] = missingset
            s1sets.setdefault(i,[]).append(key)
            s2sets.setdefault(-1,[]).append(key)
    for j in range(len(s2)):
        missingset = set()
        for item in s2[j][1]:
            if item not in setitems:
                missingset.add(item)
                setitems.append(item)
        if len(missingset) > 0:
            key = "s"+str(len(sets))
            sets[key] = missingset
            s1sets.setdefault(-1,[]).append(key)
            s2sets.setdefault(j,[]).append(key)

    moves = dict() # setkey: [from, to]
    for ind, keylist in s1sets.items():
        if ind == -1: # set gained
            for key in keylist:
                moves[key] = [-1]
        elif len(keylist) > 1:
            isets = [sets[key] for key in keylist]
            #ignore largest set
            largest = keylist[isets.index(max(isets, key=len))]
            for key in keylist:
                if key != largest:
                    moves[key] = [ind]
    for ind, keylist in s2sets.items():
        if ind == -1: # set lost
            for key in keylist:
                if key in moves:
                    moves[key].append(-1)
                else:
                    for ind2, entry in s1sets.items():
                        if key in entry:
                            moves[key] = [ind2, ind] 
        elif len(keylist) > 1:
            isets = [sets[key] for key in keylist]
            #ignore largest set - using prior tieBreaks from s1sets
            largest = tieMax(keylist, isets, moves.keys())
            for key in keylist:
                if key != largest or largest in moves:
                    if key in moves:
                        moves[key].append(ind)
                    else:
                        for ind2, entry in s1sets.items():
                            if key in entry:
                                moves[key] = [ind2, ind]
        elif keylist[0] in moves:
            moves[keylist[0]].append(ind)
    for setid, move in moves.items(): # setkey: [from, to, set] Note: -1 is special, means item gained/lost
        fromI = s1[move[0]][0] if move[0] != -1 else -1
        if len(move) < 2 or move[1] > len(s2):
            print(setid)
            print(move)
            print(sets[setid])
            print("yoo!")
        toI = s2[move[1]][0] if move[1] != -1 else -1
        moves[setid] = [fromI, toI, sets[setid]]
    return moves

class TestClusteringDiffMethods(unittest.TestCase):
    def setUp(self):
        df1 = pd.read_csv('../../data/test/highestStatus2022-01-18_regression_dev.tsv', sep="\t")
        df1.dropna(subset=['uniis'], inplace=True)
        df1.uniis = df1.uniis.apply(lambda x: "|".join(sorted(str(x).split('|'))))

        df2 = pd.read_csv('../../data/test/highestStatus2022-01-19_regression_prod.tsv', sep="\t")
        df2.dropna(subset=['uniis'], inplace=True)
        df2.uniis = df2.uniis.apply(lambda x: "|".join(sorted(str(x).split('|'))))

        dropuniis = []
        ir = df1[['uniis', 'HighestStatus']].to_records(index=False)
        ir.sort()
        jr = df2[['uniis', 'HighestStatus']].to_records(index=False)
        jr.sort()
        j = 0
        for i in range(len(ir)):
            while jr[j][0] < ir[i][0]:
                j = j + 1
            if ir[i][0] == jr[j][0]:
                dropuniis.append(ir[i][0])

        #remove identical clusters
        df1 = df1[df1.uniis.isin(dropuniis) == False]
        df2 = df2[df2.uniis.isin(dropuniis) == False]

        df1['uniisets'] = df1.uniis.apply(lambda x: set(x.split('|')))
        df2['uniisets'] = df2.uniis.apply(lambda x: set(x.split('|')))
        self.s1 = df1[['uniisets']].to_records(index=True)
        self.s2 = df2[['uniisets']].to_records(index=True)

    def testClusteringDiff(self):
        moves = clusteringDiff(self.s1, self.s2)
        self.assertTrue(len(moves) in {8,9} and \
            [91, 95, {'TLE294X33A'}] in moves.values() and \
            [92, 142, {'SW274S3321'}] in moves.values() and \
            [98, 277, {'95R1D12HEH'}] in moves.values() and \
            [105, 232, {'JB2MV9I3LS'}] in moves.values() and \
            [117, 11986, {'26MX5YAL2R'}] in moves.values() and \
            [10636, -1, {'0D21EQAQQA'}] in moves.values() and \
            [19731, -1, {'N4S59ZCS33'}] in moves.values())
        # [6154, 6155, {'CU61C5RH24', '851NLB57HQ'}] move is not deterministic, presently
        #{'s1': [91, 95, {'TLE294X33A'}], 's3': [92, 142, {'SW274S3321'}], 's6': [98, 277, {'95R1D12HEH'}], 's8': [105, 232, {'JB2MV9I3LS'}], 's10': [117, 11986, {'26MX5YAL2R'}], 's15': [6154, 6155, {'851NLB57HQ', 'CU61C5RH24'}], 's18': [10636, -1, {'0D21EQAQQA'}], 's19': [19731, -1, {'N4S59ZCS33'}]}

if __name__ == "__main__":

    args_p = argparse.ArgumentParser(description="Compare highStatus regression reports for two instances")
    args_p.add_argument('report1',
                        help="""a file address to a stitcherRegressionDF Excel report containing highestStatus""")
    args_p.add_argument('report2',
                        help="""a file address to a stitcherRegressionDF Excel report containing highestStatus""")
    args_p.add_argument('--verbose', action='store_true',
                        help="request verbose output of diffs")

    report1 = args_p.parse_args().report1
    report2 = args_p.parse_args().report2
    verbose = args_p.parse_args().verbose is not None

    df1 = readHighestStatusDF(report1)
    df2 = readHighestStatusDF(report2)

    for status in set().union(df1.HighestStatus.unique().tolist(),df2.HighestStatus.unique().tolist()):
        print(status+": "+str(df1[df1.HighestStatus == status].shape[0])+ \
            ": "+str(df2[df2.HighestStatus == status].shape[0]))

    dropuniis = []
    ir = df1[['uniis', 'HighestStatus']].to_records(index=False)
    ir.sort()
    jr = df2[['uniis', 'HighestStatus']].to_records(index=False)
    jr.sort()
    j = 0
    for i in range(len(ir)):
        while jr[j][0] < ir[i][0]:
            j = j + 1
        if ir[i][0] == jr[j][0]:
            if ir[i][1] != jr[j][1]:
                if verbose:
                    print("Status changed: "+str(ir[i])+":"+str(jr[j]))
                    print(str(df1[df1.uniis == ir[i][0]].iloc[0].tolist()))
                    print(str(df2[df2.uniis == ir[i][0]].iloc[0].tolist()))
            dropuniis.append(ir[i][0])

    print("ClusteringDiff")
    #remove identical clusters
    df1 = df1[df1.uniis.isin(dropuniis) == False]
    df2 = df2[df2.uniis.isin(dropuniis) == False]

    df1['uniisets'] = df1.uniis.apply(lambda x: set(x.split('|')))
    df2['uniisets'] = df2.uniis.apply(lambda x: set(x.split('|')))
    s1 = df1[['uniisets']].to_records(index=True)
    s2 = df2[['uniisets']].to_records(index=True)
    moves = clusteringDiff(s1, s2)

    if len(moves) > 0:
        print("These uniis have caused stitch changes:")
    for setid, move in moves.items():
        #print(move)
        for unii in move[2]:
            # all moves could affect final counts, but these are expected to be important
            if (move[0] != -1 and (unii in df1.loc[move[0]][0] or unii in df1.loc[move[0]][7])) or \
                (move[1] != -1 and (unii in df2.loc[move[1]][0] or unii in df2.loc[move[1]][7])):
                if verbose:
                    print(move[2])
                    if move[0] == -1:
                        print("***entry gained***")
                    else:
                        print(str(df1.loc[move[0]].tolist()))
                    if move[1] == -1:
                        print("***entry lost***")
                    else:
                        print(str(df2.loc[move[1]].tolist()))
                break

