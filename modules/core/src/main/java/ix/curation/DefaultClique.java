package ix.curation;

import java.util.Map;

public class DefaultClique extends DefaultComponent implements Clique {
    final Map<StitchKey, Object> values;

    public DefaultClique (Component comp) {
        this (comp, null);
    }
    
    public DefaultClique (Component comp, Map<StitchKey, Object> values) {
        super (comp);
        this.values = values;
    }

    public DefaultClique (Clique clique) {
        this (clique, clique.values());
    }

    public Map<StitchKey, Object> values () { return values; }
}
