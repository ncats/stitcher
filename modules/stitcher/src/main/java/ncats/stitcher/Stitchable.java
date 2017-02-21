package ncats.stitcher;

public interface Stitchable {
    /**
     * The propery name of associated with the stitching value
     */
    String getName ();
    
    /**
     * Value can only primitive or array of primitives. String is also
     * considered as primitive.
     */
    Object getValue (); // get raw value

    /**
     * [Optional] Return the display value
     */
    Object getDisplay ();

    // TODO Arrays are not kept in sorted order, so be careful updating displays!


    // TODO curation status
    // TODO normalized/hash value
    // TODO what normalized these values
    // TODO link to payload from whence it came?
}
