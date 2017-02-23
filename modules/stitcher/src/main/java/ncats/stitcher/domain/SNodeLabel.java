package ncats.stitcher.domain;

import org.neo4j.graphdb.Label;

import java.util.Objects;

/**
 * Created by katzelda on 2/22/17.
 */
public class SNodeLabel {
    final Label label;

    SNodeLabel(String labelname) {
        this(Label.label(labelname));
    }
    SNodeLabel(Label label) {
        this.label = Objects.requireNonNull(label);
    }


}
