package ncats.stitcher;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Array;

public class PrefixStitchKeyMapper implements StitchKeyMapper {
    final String prefix;
    final StitchKey key;

    public PrefixStitchKeyMapper (StitchKey key, String prefix) {
        this.key = key;
        this.prefix = prefix != null ? prefix : "";
    }

    public Map<StitchKey, Object> map (Object value) {
        Map<StitchKey, Object> mapped = new HashMap<>();
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                List<String> vals = new ArrayList<>();
                for (int i = 0; i < len; ++i) {
                    vals.add(prefix+Array.get(value, i));
                }
                mapped.put(key, vals.toArray(new String[0]));
            }
            else {
                mapped.put(key, prefix+value);
            }
        }
        return mapped;
    }
}
