package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public abstract class UntangleAbstract implements Props {
    static final Logger logger = Logger.getLogger
        (UntangleAbstract.class.getName());
    
    final protected DataSource dsource;
    final protected UnionFind uf = new UnionFind ();

    // current untangle entity factory; all derived classes must make sure
    // to set update this as appropriate!
    protected EntityFactory ef;
    
    protected UntangleAbstract (DataSource dsource) {
        this.dsource = dsource;
    }

    protected void dump (String mesg) {
        dump (System.out, mesg);
    }
    
    protected void dump (PrintStream ps, String mesg) {
        long[][] components = uf.components();
        ps.println("** "+mesg+": number of components: "
                   +components.length);
        for (long[] c : components) {
            ps.print(c.length+" [");
            for (int i = 0; i < c.length; ++i) {
                ps.print(c[i]);
                if (i+1 < c.length)
                    ps.print(",");
            }
            ps.println("]");
        }
    }

    static public String valueToString (Object value, char sep) {
        StringBuilder sb = new StringBuilder ();
        if (value.getClass().isArray()) {
            String[] ary = new String[Array.getLength(value)];
            for (int i = 0; i < ary.length; ++i)
                ary[i] = Array.get(value, i).toString();

            Arrays.sort(ary);
            for (int i = 0; i < ary.length; ++i) {
                if (sb.length() > 0)
                    sb.append(sep);
                sb.append(ary[i]);
            }
        }
        else {
            sb.append(value.toString());
        }

        return sep == ',' ? "["+sb+"]" : sb.toString();
    }
    
    public abstract void untangle
        (EntityFactory ef, BiConsumer<Long, long[]> consumer);
    public DataSource getDataSource () { return dsource; }    
}
