package ncats.stitcher;

import java.util.Set;
import java.util.stream.Stream;
import java.util.Iterator;

public class DefaultComponent implements Component {
    final Component comp;
    public DefaultComponent (Component comp) {
        this.comp = comp;
    }

    public String getId () { return comp.getId(); }
    public int size () { return comp.size(); }
    public Set<Long> nodeSet () { return comp.nodeSet(); }
    public Entity[] entities () { return comp.entities(); }
    public Iterator<Entity> iterator () { return comp.iterator(); }
    public Component xor (Component c) { return comp.xor(c); }
    public Component and (Component c) { return comp.and(c); }
    public Component add (Component c) { return comp.add(c); }
    public Stream<Entity> stream () { return comp.stream(); }
    public Double score () { return comp.score(); }
}
