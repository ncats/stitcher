package ncats.stitcher.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import ncats.stitcher.Component;
import ncats.stitcher.DataSource;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.EntityFactory;
import ncats.stitcher.GraphDb;
import ncats.stitcher.Stitch;
import ncats.stitcher.UntangleCompoundComponent;
import ncats.stitcher.calculators.ApprovalCalculator;
import ncats.stitcher.calculators.StitchCalculator;

public class CompoundStitcher implements Consumer<Stitch> {
    static final Logger logger = Logger.getLogger
        (CompoundStitcher.class.getName());

    
    private List<StitchCalculator> calculators = new ArrayList<>();
        
    final EntityFactory ef;
    final DataSourceFactory dsf;
        
    public CompoundStitcher (String db)  throws Exception {
        ef = new EntityFactory (GraphDb.getInstance(db));
        dsf = ef.getDataSourceFactory();
        calculators.add(new ApprovalCalculator (ef));
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


    /**
     * Add a post-processing {@link StitchCalculator} to be called after
     * a stitch is created.
     * 
     * This method returns itself for method chaining purposes.
     * 
     * @param calc
     * @return
     */
    
    public CompoundStitcher addCalculator(StitchCalculator calc){
        this.calculators.add(calc);
        
        return this;
    }


    //Perform all calculator options
        @Override
        public void accept(Stitch t) {
                calculators.stream()
                           .forEach(c->{
                                   c.accept(t);
                           });
        }
}
