package ncats.stitcher.test;

import ncats.stitcher.AuxRelType;
import ncats.stitcher.Entity;
import ncats.stitcher.Stitch;
import ncats.stitcher.StitchKey;
import ncats.stitcher.calculators.EventCalculator;
import ncats.stitcher.calculators.events.WithdrawnEventParser;
import ncats.stitcher.impl.GinasLoader;
import ncats.stitcher.impl.LineMoleculeEntityFactory;
import ncats.stitcher.impl.MapEntityFactory;
import ncats.stitcher.tools.CompoundStitcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

public class MapEntityFactoryTest {

    private static int IGNORED_INT = -1;
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    @Rule
    public TestRegistryResource<LineMoleculeEntityFactory> testRegistryResource = TestRegistryResource.createFromTempFolder( tmpDir, f-> new LineMoleculeEntityFactory(f));


    @Test
    public void registerFromFile() throws Exception{
        LineMoleculeEntityFactory registry = testRegistryResource.getRegistry();
        registry.setStrucField("smiles");

//        registry.setIdField("unii");
        registry.setNameField("generic_name");

        registry.add(StitchKey.I_UNII, "unii");
        registry.add(StitchKey.N_Name, "generic_name");

        File f = new File(getClass().getResource("/combined_withdrawn_shortage_drugs.txt").getFile());
        registry.setDataSource(registry.getDataSourceFactory().register(f));

        try(InputStream in  = new BufferedInputStream(new FileInputStream(f))) {
            registry.register(in, "\t", IGNORED_INT);
        }

        new CompoundStitcher(registry)
                .stitch(1);

        EventCalculator ac = new EventCalculator(registry);
//        ac.setEventParsers(Arrays.asList(new WithdrawnEventParser()));
        ac.recalculate(1);


        registry.entities(registry.getDataSource(), e->{
                System.out.println(e.get(StitchKey.I_UNII));
                Node node = e._node();
                for (Relationship r : node.getRelationships(AuxRelType.EVENT)) {
                    Node dataNode = r.getOtherNode(node);
                    System.out.println("\tproperties : " + dataNode.getAllProperties());
                }
           });

    }


}
