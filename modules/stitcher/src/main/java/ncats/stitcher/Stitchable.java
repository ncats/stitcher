package ncats.stitcher;

public interface Stitchable {
    static public final int ALL = 0;
    static public final int ANY = 1;
    static public final int SINGLE = 2;

    /*
     * The propery name of associated with the stitching value
     */
    String getName ();
    
    /*
     * Value can only primitive or array of primitives. String is also
     * considered as primitive.
     */
    Object getValue (); // get raw value

    /*
     * [Optional] Return the display value
     */
    Object getDisplay ();

    default boolean isBlacklist (StitchKey key) {
        return false;
    }

    default boolean isBlacklist () { return false; }
    
    // TODO Arrays are not kept in sorted order, so be careful updating displays!
    // TODO curation status
    // TODO normalized/hash value
    // TODO what normalized these values
    // TODO link to payload from whence it came?
}
