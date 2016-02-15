package ix.curation;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class DefaultPayload implements Payload {
    protected DataSource source;
    protected Object id;
    protected Set<String> indexes = new HashSet<String>();
    protected Map<String, Object> data = new HashMap<String, Object>();

    public DefaultPayload (DataSource source) {
        this (source, null);
    }
    
    public DefaultPayload (DataSource source, Object id) {
        if (source == null)
            throw new IllegalArgumentException ("Data source is null");
        this.source = source;
        this.id = id;
    }

    public Object getId () { return id; }
    public DefaultPayload setId (Object id) {
        this.id = id;
        return this;
    }
    
    public DataSource getSource () { return source; }

    public Map<String, Object> getData () { return data; }
    public DefaultPayload put (String prop, Object value) {
        return put (prop, value, true);
    }

    public DefaultPayload putAll (Map<String, Object> payload) {
        for (Map.Entry<String, Object> me : payload.entrySet())
            put (me.getKey(), me.getValue(), true);
        return this;
    }

    public Set<String> getIndexes () { return indexes; }    
    public DefaultPayload addIndexes (String... names) {
        for (String n : names)
            indexes.add(n);
        return this;
    }
    
    public DefaultPayload put (String prop, Object value, boolean index) {
        if (index)
            indexes.add(prop);
        data.put(prop, value);
        return this;
    }

    public boolean has (String prop) { return data.containsKey(prop); }
}
