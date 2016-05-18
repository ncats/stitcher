package ix.curation;

import java.util.function.Consumer;

/**
 * interface defining for entity resolution
 */
public interface EntityResolution {
    /*
     * resolve a given entity into one or more of its constituent components
     */
    void resolve (EntityFactory ef, Consumer<Entity[]> component);
}
