package ix.curation;

import java.util.Map;

public interface CurationMetrics {
    // total number of entities
    int getEntityCount ();

    // histogram of entities; sum equals to entity count
    Map<EntityType, Integer> getEntityHistogram ();

    // distribution of entity size (associated stitch count) 
    Map<Integer, Integer> getEntitySizeDistribution ();

    // total number of stitches
    int getStitchCount ();

    // histogram of stitch keys; sum equals to stitch count
    Map<StitchKey, Integer> getStitchHistogram ();

    // total number of connected components; this includes singletons
    int getConnectedComponentCount ();

    // histogram of CC sizes
    Map<Integer, Integer> getConnectedComponentHistogram ();

    // number of connected components for which size = 1
    int getSingletonCount ();
}
