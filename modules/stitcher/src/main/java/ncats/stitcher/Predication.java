package ncats.stitcher;
import java.util.Map;

public interface Predication {
    public int score
        (Entity source, Map<StitchKey,Object> stitches, Entity target);
}
