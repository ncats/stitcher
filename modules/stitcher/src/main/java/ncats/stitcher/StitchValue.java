package ncats.stitcher;

import java.io.Serializable;
import java.lang.reflect.Array;

public class StitchValue implements Stitchable {
    protected String name; // name of property from payload
    protected Object value;
    protected Object display;

    public StitchValue (Object value) {
        this (null, value);
    }

    public StitchValue (String name, Object value) {
        this.name = name;
        this.value = value;
    }

    // TODO Arrays are not kept in sorted order, so be careful updating displays!

    // TODO Assume that if display is not provided, then you can get it from payload (no need to store molfile twice)

    public StitchValue (String name, Object value, Object display) {
        this.name = name;
        this.value = value;
        this.display = display;
    }

    public void setValue (Object value) {
        this.value = value;
    }
    public Object getValue () { return value; }

    public String getName () { return name; }
//    public void setName (String name) { this.name = name; }
    
    public Object getDisplay () { return display;}
    public void setDisplay (String display) { this.display = display; }
}
