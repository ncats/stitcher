package services;

import java.io.InputStream;
import java.util.*;

import play.Logger;

public abstract class RecordScanner<T> implements Iterator<T> {
    static final String[] NO_PROPS = new String[0];
    static final int DEFAULT_MAX_SCAN = 1000;
    
    protected int max;
    protected int count;
    protected T current;
    protected String[] properties = NO_PROPS;
    protected Map<String, Integer> counts =
        new HashMap<String, Integer>();
    protected Map<String, Class> types = new HashMap<String, Class>();

    protected RecordScanner () {
        this (DEFAULT_MAX_SCAN);
    }

    protected RecordScanner (int max) {
        this.max = max;
    }

    protected void reset () throws Exception {
        current = getNext ();
        count = 0;
        properties = NO_PROPS;
        types.clear();
        counts.clear();
    }
    
    public void setInputStream (InputStream is) throws Exception {
        reset ();
    }

    public int getMaxScanned () { return max; }
    public void setMaxScanned (int max) { this.max = max; }
    public int getCount () { return count; }
    public String[] getProperties () { return properties; }
    public Integer getCount (String prop) {
        return counts.get(prop);
    }
    public Class getType (String prop) { return types.get(prop); }
    
    @Override
    public boolean hasNext () {
        return current != null;
    }

    public void scan () {
        for (int n = 0; (max < 0 || n < max) && hasNext (); ++n)
            next ();
    }

    @Override
    public T next () {
        if (current == null)
            throw new IllegalStateException ("No more records available!");

        String[] props = getProperties (current);
        if (props.length > properties.length)
            properties = props;
        
        for (String p : props) {
            Integer c = counts.get(p);
            counts.put(p, c != null ? c+1 : 1);
        }
        
        T next = null;
        ++count;
        try {
            next = getNext ();
        }
        catch (Exception ex) {
            Logger.trace(getClass()+": can't get next recored", ex);
        }

        T v = current;
        current = next;
        
        return v;
    }

    /**
     * return a list of properties for which the given record contains.
     * subclass should override this method.
     */
    protected String[] getProperties (T record) {
        return NO_PROPS;
    }
    
    protected abstract <T> T getNext () throws Exception;
}
