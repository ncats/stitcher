package ncats.stitcher;

import java.util.*;

public class BlacklistStitchKeyMapper implements StitchKeyMapper {
    protected Set blacklist = new HashSet ();
    protected final StitchKey key;

    protected BlacklistStitchKeyMapper () {
        this (null);
    }
    public BlacklistStitchKeyMapper (StitchKey key) {
        this.key = key;
    }

    public void addBlacklist (Object value) {
        if (value instanceof String)
            value = ((String)value).toUpperCase();
        blacklist.add(value);
    }
    
    public void removeBlacklist (Object value) {
        if (value instanceof String)
            blacklist.remove(((String)value).toUpperCase());
        else
            blacklist.remove(value);
    }
    
    public boolean isBlacklist (Object value) {
        return value instanceof String
            ? blacklist.contains(value.toString().toUpperCase())
            : blacklist.contains(value);
    }
    
    public Map<StitchKey, Object> map (Object value) {
        Map<StitchKey, Object> mapped = new HashMap<>();
        if (!isBlacklist (value)) {
            mapped.put(key, value);
        }
        return mapped;
    }
}
