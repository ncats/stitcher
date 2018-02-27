package ncats.stitcher.tools;

import ncats.stitcher.*;
import ncats.stitcher.calculators.CalculatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.net.URI;

public class CompoundStitcher implements Consumer<Stitch> {
    static final Logger logger = Logger.getLogger
        (CompoundStitcher.class.getName());

    static class StitcherUntangleCompoundComponent
        extends UntangleCompoundComponent {
        public StitcherUntangleCompoundComponent
            (DataSource dsource, Component component) {
            super (dsource, component);
        }

        @Override
        protected Long getRoot (long[] comp) {
            Long root = super.getRoot(comp), r = root;
            
            if (r != null) {
                // make sure it's a
                Entity[] e = component.entities(new long[]{root});
                URI uri = e[0].datasource().toURI();
                if (uri != null && !uri.toString().endsWith(".gsrs"))
                    r = null;
            }
            
            if (r == null) {
                for (Entity e : component.entities(comp)) {
                    URI uri = e.datasource().toURI();
                    if (uri != null && uri.toString().endsWith(".gsrs")) {
                        if (r == null || r > e.getId())
                            r = e.getId();
                    }
                }
            }
            
            return r != null ? r : root;
        }
    }
    
    final EntityFactory ef;
    final DataSourceFactory dsf;

    public CompoundStitcher(EntityFactory ef){
        this.ef = ef;
        this.dsf = ef.getDataSourceFactory();
    }
    public CompoundStitcher (String db)  throws Exception {
        ef = new EntityFactory (GraphDb.getInstance(db));
        dsf = ef.getDataSourceFactory();
    }

    public void shutdown () {
        ef.shutdown();
    }

    public void stitch (int version, Long... components) throws Exception {
        DataSource dsource = dsf.register("stitch_v"+version);
        int count = 1;
        if (components == null || components.length == 0) {
            // do all components
            logger.info("Untangle all components...");
            List<Long> comps = new ArrayList<>();
            int total = ef.components(comps);
            logger.info("### "+total+" components!");
            for (Long cid : comps) {
                logger.info("################ UNTANGLE COMPONENT "+cid
                            +" of size "+ef.entity(cid).get(Props.RANK)
                            +"... "+count+"/"+total);
                Component comp = ef.component(cid);
                logger.info("Stitching component "+cid);
                ef.untangle(new StitcherUntangleCompoundComponent
                            (dsource, comp), this);
                ++count;
            }
        }
        else {
            for (Long cid : components) {
                logger.info("################ UNTANGLE COMPONENT "+cid
                            +" of size "+ef.entity(cid).get(Props.RANK)
                            +"... "+count+"/"+components.length);
                Component comp = ef.component(cid);
                logger.info("Stitching component "+comp.getId());
                ef.untangle(new StitcherUntangleCompoundComponent
                            (dsource, comp), this);
                ++count;
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
