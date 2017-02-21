package ncats.stitcher;

import java.util.List;
import java.util.logging.Logger;

public class DefaultEntityVisitor extends AbstractEntityVisitor {
    static final Logger logger =
        Logger.getLogger(DefaultEntityVisitor.class.getName());
    
    public DefaultEntityVisitor () {
    }
    public DefaultEntityVisitor (StitchKey... keys) {
        super (keys);
    }
    
    public boolean visit (Entity[] path, Entity e) {
        logger.info("## Entity visited "+e);
        return true;
    }
}
