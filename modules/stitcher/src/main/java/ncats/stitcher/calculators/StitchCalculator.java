package ncats.stitcher.calculators;

import java.util.Arrays;
import java.util.function.Consumer;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Stitch;

public abstract class StitchCalculator implements Consumer<Stitch>{
    EntityFactory ef;
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Stitch.class.getName());

    public int recalculate (int version) {
        logger.info("## recalculating stitch_v"+version+"...");
        return ef.maps(e -> {
            Stitch s = Stitch._getStitch(e);
            accept (s);
        }, "S_STITCH_V"+version);
    }

    public int recalculateNodes (long[] nodes) {
        logger.info("## recalculating node "+ Arrays.toString(nodes)+"...");
        return ef.maps(e -> {
            Stitch s = Stitch._getStitch(e);
            accept (s);
        }, nodes);
    }

}
