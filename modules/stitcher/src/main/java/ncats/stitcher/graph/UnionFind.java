package ncats.stitcher.graph;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Comparator;

/**
 * UnionFind algorithm with path compression
 */
public class UnionFind {

    private Map<Long, Long> parent = new HashMap<Long, Long>();
    private Map<Long, Integer> rank = new HashMap<Long, Integer>();

    public UnionFind () {
    }

    public void clear () {
        parent.clear();
        rank.clear();
    }

    protected Long getRoot (Long n) {
        return getRoot (n, 1);
    }
    
    protected Long getRoot (Long n, int r) {
        Long p = parent.get(n);
        if (p != null) {
            while (!n.equals(p)) {
                n = p;          
                p = parent.get(p);              
            }
        }
        else {
            parent.put(n, n);
            rank.put(n, r);
        }
        return n;
    }

    public boolean add (long p) {
        return add (p, 1);
    }
    
    public boolean add (long p, int r) {
        Long q = getRoot (p, r);
        return q.equals(p);
    }
    
    public boolean find (long p, long q) {
        return parent.containsKey(p) && parent.containsKey(q)
            && getRoot(p).equals(getRoot (q));
    }

    public boolean contains (long p) {
        return parent.containsKey(p);
    }

    public Long root (long p) {
        return parent.containsKey(p) ? getRoot (p) : null;
    }

    public long union (long p, long q) {
        return union (p, q, true);
    }

    public long union (long p, long q, boolean maxrank) {
        long i = getRoot (p);
        long j = getRoot (q);
        long k;
        if (i != j) {
            int ri = rank.get(i);
            int rj = rank.get(j);
            if (ri >= rj || !maxrank) { // preserve directionality
                parent.put(j, i);
                rank.put(i, ri+rj);
                k = i;
            }
            else {
                parent.put(i, j);
                rank.put(j, ri+rj);
                k = j;
            }
        }
        else
            k = i; // or p = j
        return k;
    }

    public long component (long p) { return getRoot (p); }
    public long[][] components () {
        Map<Long, Set<Long>> eqmap = new TreeMap<Long, Set<Long>>();
        for (Long n : parent.keySet()) {
            Long p = getRoot (n);
            Set<Long> comp = eqmap.get(p);
            if (comp == null) {
                eqmap.put(p, comp = new TreeSet<Long>());
            }
            comp.add(n);
        }

        // equivalence class
        long[][] eqv = new long[eqmap.size()][];
        int eq = 0;
        for (Map.Entry<Long, Set<Long>> me : eqmap.entrySet()) {
            Set<Long> v = me.getValue();
            eqv[eq] = new long[v.size()];
            int i = 0;
            for (Iterator<Long> it = v.iterator(); it.hasNext(); ++i)
                eqv[eq][i] = it.next();
            ++eq;
        }

        // now sort the array
        Arrays.sort(eqv, new Comparator<long[]> () {
                public int compare (long[] c1, long[] c2) {
                    int d = c2.length - c1.length;
                    if (d == 0) {
                        d = (int)(c1[0] - c2[0]);
                    }
                    return d;
                }
            });

        return eqv;
    }
}
