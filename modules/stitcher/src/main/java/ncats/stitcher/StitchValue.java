package ncats.stitcher;

import java.io.Serializable;
import java.lang.reflect.Array;

public class StitchValue implements Stitchable, Comparable<StitchValue> {
    protected StitchKey key;
    protected String name; // name of property from payload
    protected Object value;
    protected Object display;

    public StitchValue (Object value) {
        this.value = value;
    }

    public StitchValue (String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public StitchValue (StitchKey key, Object value) {
        this (key, key.name(), value);
    }

    public StitchValue (StitchKey key, String name, Object value) {
        this.key = key;
        this.name = name;
        this.value = value;
    }

    public StitchValue (String name, Object value, Object display) {
        this.name = name;
        this.value = value;
        this.display = display;
    }

    public boolean equals (Object obj) {
        if (this == obj) return true;
        if (obj instanceof StitchValue) {
            StitchValue sv = (StitchValue)obj;
            if ((key != null && !key.equals(sv.key))
                || (key == null && sv.key != null)
                || (name != null && !name.equals(sv.name))
                || (name == null && sv.name != null))
                return false;
            return value != null && value.equals(sv.value);
        }
        return false;
    }

    public int compareTo (StitchValue sv) {
        int d = 0;
        if (key != null && sv.key != null)
            d = key.name().compareTo(sv.key.name());
        if (d == 0 && name != null)
            d = name.compareTo(sv.name);
        return d;
    }

    public int hashCode () {
        return (key != null ? key.hashCode() : 123)
            ^ (name != null ? name.hashCode() : 456)
            ^ (value != null ? value.hashCode() : 789);
    }

    public StitchKey getKey () { return key; }
    public String getName () { return name; }
    
    public void setValue (Object value) { this.value = value; }
    public Object getValue () { return value; }
    public Object getDisplay () { return display;}
    public void setDisplay (String display) { this.display = display; }
    public String toString () {
        return "{key="+key+",name="+name+",value="+value+"}";
    }
}
