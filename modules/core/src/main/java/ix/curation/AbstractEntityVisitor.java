package ix.curation;

import java.util.EnumMap;
import java.util.Map;

public abstract class AbstractEntityVisitor implements EntityVisitor {
    public static final Object NONE = new Object ();
    protected EnumMap<StitchKey, Object> keys = new EnumMap<>(StitchKey.class);

    protected AbstractEntityVisitor () {}
    protected AbstractEntityVisitor (StitchKey... keys) {
        for (StitchKey key : keys) {
            this.keys.put(key, NONE);
        }
    }

    public void clear () { keys.clear(); }
    public void set (StitchKey key, Object value) {
        if (value != null) {
            keys.put(key, value);
        }
        else
            keys.remove(key);
    }
    
    public void set (StitchKey key) {
        keys.put(key, NONE);
    }

    public boolean contains (StitchKey key) {
        return keys.containsKey(key);
    }

    public Object get (StitchKey key) {
        return keys.get(key);
    }

    /**
     * EntityVisitor interface
     */
    public boolean next (StitchKey key, Object value) {
        if (keys.isEmpty())
            return false;
        
        Object val = keys.get(key);
        return val == NONE || (val != null && val.equals(value));
    }

    public abstract boolean visit (Entity[] path, Entity e);
}
