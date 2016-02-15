package ix.curation;

/**
 * @author Rajarshi Guha
 */
public class Target {
    public String id, name, acc;
    public TargetType type;
    public Integer taxId; // NCBI Taxonomy ID

    @Override
    public String toString() {
        return "Target{" +
                "id="+id+
                ", name='" + name + '\'' +
                ", acc='"+acc+'\''+
                '}';
    }
}
