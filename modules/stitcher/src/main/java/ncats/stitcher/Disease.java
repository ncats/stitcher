package ncats.stitcher;

/**
 * @author Rajarshi Guha
 */
public class Disease {
    public String name, description, source, id;
    public String[] uniProtIds = null;

    @Override
    public String toString() {
        return "Disease{" +
                "name='" + name + '\'' +
                ", source='" + source + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
