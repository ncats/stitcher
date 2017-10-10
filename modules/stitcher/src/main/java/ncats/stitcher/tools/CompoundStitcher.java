package ncats.stitcher.tools;

import ncats.stitcher.*;
import ncats.stitcher.calculators.CalculatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class CompoundStitcher implements Consumer<Stitch> {
    static final Logger logger = Logger.getLogger
        (CompoundStitcher.class.getName());

    
    final EntityFactory ef;
    final DataSourceFactory dsf;
        
    public CompoundStitcher (String db)  throws Exception {
        ef = new EntityFactory (GraphDb.getInstance(db));
        dsf = ef.getDataSourceFactory();
    }

    public void shutdown () {
        ef.shutdown();
    }

    public void stitch (int version, Long... components) throws Exception {
        DataSource dsource = dsf.register("stitch_v"+version);
        if (components == null || components.length == 0) {
            // do all components
            logger.info("Untangle all components...");
            List<Long> comps = new ArrayList<>();
            ef.components(component -> {
                    comps.add(component.root().getId());
                });
            logger.info("### "+components.length+" components!");
            for (Long cid : comps) {
                logger.info("########### Untangle component "+cid+"...");
                ef.untangle(new UntangleCompoundComponent
                            (dsource, ef.component(cid)), this);
            }
        }
        else {
            for (Long cid : components) {
                Component comp = ef.component(cid);
                logger.info("Stitching component "+comp.getId());
                ef.untangle(new UntangleCompoundComponent
                            (dsource, comp), this);
            }
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+CompoundStitcher.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }

        CompoundStitcher cs = new CompoundStitcher (argv[0]);
        int version = Integer.parseInt(argv[1]);

        List<Long> comps = new ArrayList<>();
        for (int i = 2; i < argv.length; ++i)
            comps.add(Long.parseLong(argv[i]));
        cs.stitch(version, comps.toArray(new Long[0]));

        cs.shutdown();
    }

    // Perform all calculator options
    @Override
    public void accept(Stitch t) {
        CalculatorFactory.getCalculatorFactory(ef)
                         .process(Stitch.getStitch(t));
    }
}
