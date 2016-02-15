package ix.curation;

import java.util.Map;
import java.util.Set;

public interface Payload {
    /**
     * Return the source of the payload. This is a required property and
     * can't be null.
     */
    DataSource getSource ();

    /**
     * Return the id of the payload. This is a required property and
     * can't be null. 
     */
    Object getId ();
    
    /**
     * Return which of the properties in from getData to index; this is 
     */
    Set<String> getIndexes ();
    Map<String, Object> getData ();
}
