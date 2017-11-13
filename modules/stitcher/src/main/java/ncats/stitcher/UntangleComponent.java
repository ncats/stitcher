package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public abstract class UntangleComponent implements Props {
    static final Logger logger = Logger.getLogger
        (UntangleComponent.class.getName());
    
    final protected DataSource dsource;
    final protected Component component;
    final protected UnionFind uf = new UnionFind ();
    final protected Map<StitchKey, Map<Object, Integer>> stats;

    protected UntangleComponent (DataSource dsource, Component component) {
        stats = new HashMap<>();
        for (StitchKey key : Entity.KEYS) {
            stats.put(key, component.stats(key));
        }
        
        long created = (Long) dsource.get(CREATED);
        for (Entity e : component) {
            long c = (Long) e.get(CREATED);
            if (c > created) {
                throw new IllegalArgumentException
                    ("Can't use DataSource \""+dsource.getName()+"\" ("
                     +dsource.getKey()+") "
                     +"as reference to untangle component "
                     +component.getId()+" because data has changed "
                     +"since it was last created!");
            }
        }
        this.component = component;
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
    public Component component () { return component; }
    public DataSource getDataSource () { return dsource; }
}
