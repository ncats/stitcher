package ncats.stitcher;

import java.util.Map;

public interface StitchKeyMapper {
    Map<StitchKey, Object> map (Object value);
}
