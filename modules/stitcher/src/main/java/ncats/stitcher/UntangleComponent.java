package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public abstract class UntangleComponent {
    static final Logger logger = Logger.getLogger
        (UntangleComponent.class.getName());

    final protected Component component;
    final protected UnionFind uf = new UnionFind ();
    final protected Map<StitchKey, Map<Object, Integer>> stats;

    protected UntangleComponent (Component component) {
        stats = new HashMap<>();
        for (StitchKey key : Entity.KEYS) {
            stats.put(key, component.stats(key));
        }
        this.component = component;
    }

    protected void dump (String mesg) {
        long[][] components = uf.components();
        System.out.println("** "+mesg+": number of components: "
                           +components.length);
        for (long[] c : components) {
            System.out.print(c.length+" [");
            for (int i = 0; i < c.length; ++i) {
                System.out.print(c[i]);
                if (i+1 < c.length)
                    System.out.print(",");
            }
            System.out.println("]");
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
    
    public abstract void untangle (BiConsumer<Long, long[]> consumer);
    public Component component () { return component; }
    public long[][] getComponents () { return uf.components(); }    
}
