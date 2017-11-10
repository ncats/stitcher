package ncats.stitcher.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.*;
import ncats.stitcher.impl.*;
import org.neo4j.graphdb.GraphDatabaseService;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

public class TestDataSourceFactory {
    static final Logger logger =
        Logger.getLogger(TestDataSourceFactory.class.getName());

    static {
        GraphDb.addShutdownHook();
    }
    
    @Rule public TestName name = new TestName();
    public TestDataSourceFactory () {
    }

    @Test
    public void testURL () throws Exception {
        DataSourceFactory dsf =
            new DataSourceFactory (GraphDb.createTempDb(name.getMethodName()));
        DataSource ds = dsf.register
            (new URL("https://tripod.nih.gov/files/npc_samples_20151211.sdf"));
        assertTrue ("7878f78dea84ea0252f7c5ca7de1fc02c3d14847"
                    .startsWith(ds.getKey()));
    }
    
    @Test
    public void testSRS () throws Exception {
        SRSJsonEntityFactory srs = new SRSJsonEntityFactory
            (GraphDb.createTempDb(name.getMethodName()));
        URL url = DataSourceFactory.class.getResource
            ("/jsonDumpINN100.txt.gz");
        srs.register(url);
        
        GraphMetrics metrics = srs.calcGraphMetrics();
        assertTrue ("Expecting entity count to be 100 but instead got "
                    +metrics.getEntityCount(), metrics.getEntityCount() == 100);
        assertTrue ("Expecting stitch count to be 26 but instead got "
                    +metrics.getStitchCount(), metrics.getStitchCount() == 26);
        assertTrue ("Expecting connected component count to be 93 but instead "
                    +"got "+metrics.getConnectedComponentCount(),
                    metrics.getConnectedComponentCount() == 93);
        assertTrue ("Expecting singleton count to be 90 but instead got "
                    +metrics.getSingletonCount(), 
                    metrics.getSingletonCount() == 90);
        srs.shutdown();
    }

    @Test
    public void testNPC () throws Exception {
        NPCEntityFactory nef =
            new NPCEntityFactory(GraphDb.createTempDb
                                 (name.getMethodName()));
        nef.register(getClass().getResource("/aspirin_npc.sdf"));
        
        String[] datasets = {
            "Canada",
            "ClinicalTrials.gov",
            "USAN",
            "FDA DailyMed",
            "NPC screening",
            "Approved drugs",
            "FDA maximum daily dose",
            "FDA orange book",
            "FDA OTC",
            "Human approved drugs",
            "WHO essential",
            "FDA approved",
            "DrugBank v3.0",
            "Tariff",
            "KEGG",
            "FDA NDC",
            "EMA orphan",
            "FDA drugs@FDA",
            "FDA green book",
            "NPC informatics",
            "UK NHS",
            "FDA human approved",
            "HTS amenable drugs",
            "INN",
            "Japan"
        };
        
        nef.maps(e -> {
                Set<String> labels = e.labels();
                for (String d : datasets) {
                    assertTrue ("Expecting entity to have the label \""+d
                                +"\" but instead got "+labels, 
                                labels.contains(d));
                }
            }, AuxNodeType.ENTITY.name());
        nef.shutdown();
    }
    
    @Test
    public void testMultiDataSources () throws Exception {
        GraphDb graphDb = GraphDb.createTempDb(name.getMethodName());
        
        SRSJsonEntityFactory srs = new SRSJsonEntityFactory (graphDb);
        srs.register(getClass().getResource("/aspirin_srs.txt"));
        
        MoleculeEntityFactory mef = new MoleculeEntityFactory (graphDb);
        mef.setIdField("ncgc_id");
        mef.add(new StitchKeyMapper () {
                public Map<StitchKey, Object> map (Object value) {
                    Map<StitchKey, Set<String>> values =
                        new HashMap<StitchKey, Set<String>>();
                    for (String s : value.toString().split("\n")) {
                        if (s.startsWith("DSSTox")) {
                            // ignore
                        }
                        else if (s.startsWith("CAS-")) {
                            Set set = values.get(StitchKey.I_CAS);
                            if (set == null) {
                                values.put(StitchKey.I_CAS,
                                           set = new HashSet ());
                            }
                            set.add(s.substring(4));
                        }
                        else {
                            Set set = values.get(StitchKey.N_Name);
                            if (set == null) {
                                values.put(StitchKey.N_Name,
                                           set = new HashSet());
                            }
                            
                            // we should have a normalization factory
                            set.add(s.toUpperCase());
                        }
                    }
                    
                    EnumMap<StitchKey, Object> stitches =
                        new EnumMap<StitchKey, Object>(StitchKey.class);
                    for (Map.Entry<StitchKey, Set<String>> me
                             : values.entrySet()) {
                        stitches.put(me.getKey(),
                                     me.getValue().toArray(new String[0]));
                    }
                    return stitches;
                }
            }, "PUBCHEM_SUBSTANCE_SYNONYM");
        mef.register(getClass().getResource("/aspirin_tox.sdf"));
        // this shouldn't register anything
        mef.register(getClass().getResource("/aspirin_tox.sdf"));
        
        mef.setIdField("ID");
        mef.add(StitchKey.N_Name, "Synonyms");
        mef.add(StitchKey.I_CAS, "CAS");
        mef.add(StitchKey.I_UNII, "CompoundUNII");
        mef.add(StitchKey.I_CID, "CompoundCID");
        mef.register(getClass().getResource("/aspirin_npc.sdf"));

        final List<Entity[]> cliques = new ArrayList<Entity[]>();
        mef.cliques(clique -> {
                    logger.info("+++++ Clique ("+clique.size()
                                +") found for "+clique.values()+" ++++++");
                    for (Entity e : clique.entities()) {
                        logger.info("  "+e.getId()+": "+e.datasource());
                    }
                    cliques.add(clique.entities());
                    return true;
                });
        assertTrue ("There should have been 32 cliques found but instead got "
                    +cliques.size(), cliques.size() == 32);
                
        graphDb.shutdown();
    }
}
