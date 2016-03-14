package ix.curation;

import java.util.*;
import java.util.regex.*;
import java.lang.reflect.Array;

public class RegexStitchKeyMapper implements StitchKeyMapper {
    Map<StitchKey, List<Pattern>> stitches =
        new EnumMap<StitchKey, List<Pattern>>(StitchKey.class);
    
    public RegexStitchKeyMapper () {
    }

    public int size () { return stitches.size(); }
    public boolean isEmpty () { return stitches.isEmpty(); }

    public void add (StitchKey key, String pattern) {
        List<Pattern> patterns = stitches.get(key);
        if (patterns == null) {
            stitches.put(key, patterns = new ArrayList<Pattern>());
        }
        
        try {
            Pattern p = Pattern.compile(pattern);
            patterns.add(p);
        }
        catch (Exception ex) {
            throw new IllegalArgumentException (ex);
        }
    }
    
    public Map<StitchKey, Object> map (String value) {
        Map<StitchKey, Object> mapped = new HashMap<StitchKey, Object>();
        
        for (Map.Entry<StitchKey, List<Pattern>> me : stitches.entrySet()) {
            StitchKey key = me.getKey();
            List values = new ArrayList ();
            for (Pattern p : me.getValue()) {
                Matcher m = p.matcher(value);
                while (m.find()) {
                    String v = m.group().trim().replaceAll("\"", "");
                    if (key.getType() == Long.class) {
                        try {
                            long lv = Long.parseLong(v);
                            if (values.indexOf(lv) < 0)
                                values.add(lv);
                        }
                        catch (NumberFormatException ex) {
                        }
                    }
                    else {
                        if (values.indexOf(v) < 0)
                            values.add(v);
                    }
                }
            }
            
            if (values.isEmpty()) {
            }
            else if (values.size() == 1) {
                mapped.put(key, values.iterator().next());
            }
            else {
                Object ary = Array.newInstance(key.getType(), values.size());
                for (int i = 0; i < values.size(); ++i)
                    Array.set(ary, i, values.get(i));
                mapped.put(key, ary);
            }
        }
        
        return mapped;
    }
}
