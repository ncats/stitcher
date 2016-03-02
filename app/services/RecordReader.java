package services;

import java.io.InputStream;
import java.util.*;

import play.Logger;

public abstract class RecordReader<T> implements Iterator<T> {
    static final String[] NO_PROPS = new String[0];
    
    protected InputStream is;
    protected int count;
    protected T current;
    protected Map<String, Integer> properties =
        new HashMap<String, Integer>();

    protected RecordReader () { }

    public void setInputStream (InputStream is) throws Exception {
        this.is = is;
        current = getNext ();
        count = 0;
    }

    public int getCount () { return count; }
    public Map<String, Integer> getProperties () { return properties; }
    
    @Override
    public boolean hasNext () {
        return current != null;
    }

    @Override
    public T next () {
        if (current == null)
            throw new IllegalStateException ("No more records available!");

        for (String p : getProperties (current)) {
            Integer c = properties.get(p);
            properties.put(p, c != null ? c+1 : 1);
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
