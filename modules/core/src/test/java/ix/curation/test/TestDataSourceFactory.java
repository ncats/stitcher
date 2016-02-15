package ix.curation.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;

import ix.curation.*;
import ix.curation.impl.*;
import org.neo4j.graphdb.GraphDatabaseService;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

public class TestDataSourceFactory {
    static final Logger logger =
        Logger.getLogger(TestDataSourceFactory.class.getName());
    
    @Rule public TestName name = new TestName();

    static final File TEMPDIR = new File (".");
    
    public TestDataSourceFactory () {
    }

    @Test
    public void testURL () throws Exception {
        DataSourceFactory dsf =
            new DataSourceFactory (GraphDb.createTempDb
                                   (name.getMethodName(), TEMPDIR));
        DataSource ds = dsf.register
            (new URL("https://tripod.nih.gov/files/npc_samples_20151211.sdf"));
        assertTrue ("7878f78dea84ea0252f7c5ca7de1fc02c3d14847"
                    .startsWith(ds.getKey()));
    }
    
    @Test
    public void testSRS () throws Exception {
        SRSJsonEntityFactory srs = new SRSJsonEntityFactory
            (GraphDb.createTempDb(name.getMethodName(), TEMPDIR));
        URL url = DataSourceFactory.class.getResource
            ("/jsonDumpINN100.txt.gz");
        srs.register(url);
        
        CurationMetrics metrics = srs.calcCurationMetrics();
        assertTrue ("Expecting entity count to be 99 but instead got "
                    +metrics.getEntityCount(), metrics.getEntityCount() == 99);
        assertTrue ("Expecting stitch count to be 0 but instead got "
                    +metrics.getStitchCount(), metrics.getStitchCount() == 0);
        assertTrue ("Expecting connected component count to be 99 but instead "
                    +"got "+metrics.getConnectedComponentCount(),
                    metrics.getConnectedComponentCount() == 99);
        assertTrue ("Expecting singleton count to be 99 but instead got "
                    +metrics.getSingletonCount(),
                    metrics.getSingletonCount() == 99);
    }

    @Test
    public void testNPC () throws Exception {
        NPCEntityFactory nef =
            new NPCEntityFactory(GraphDb.createTempDb
                                 (name.getMethodName(), TEMPDIR));
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
        
        for (Iterator<Entity> it = nef.entities(); it.hasNext(); ) {
            Entity e = it.next();
            Set<String> labels = e.labels();
            for (String d : datasets) {
                assertTrue ("Expecting entity to have the label \""+d+"\"", 
                            labels.contains(d));
            }
        }
        nef.shutdown();
    }
    
    @Test
    public void testMultiDataSources () throws Exception {
        GraphDb graphDb = GraphDb.createTempDb
            (name.getMethodName(), TEMPDIR);
        
        new SRSJsonEntityFactory (graphDb)
            .register(getClass().getResource("/aspirin_srs.txt"));
        
        MoleculeEntityFactory mef = new MoleculeEntityFactory (graphDb);
        mef.setId("ncgc_id");
        mef.add("PUBCHEM_SUBSTANCE_SYNONYM", new StitchKeyMapper () {
                public Map<StitchKey, Object> map (String value) {
                    Map<StitchKey, Set<String>> values =
                        new HashMap<StitchKey, Set<String>>();
                    for (String s : value.split("\n")) {
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
                            Set set = values.get(StitchKey.N_Synonym);
                            if (set == null) {
                                values.put(StitchKey.N_Synonym,
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
            });
        mef.register(getClass().getResource("/aspirin_tox.sdf"));
        // this shouldn't register anything
        mef.register(getClass().getResource("/aspirin_tox.sdf"));
        
        mef.setId("ID");
        mef.add(StitchKey.N_Synonym, "Synonyms");
        mef.add(StitchKey.I_CAS, "CAS");
        mef.add(StitchKey.I_UNII, "CompoundUNII");
        mef.add(StitchKey.I_CID, "CompoundCID");
        mef.register(getClass().getResource("/aspirin_npc.sdf"));

        final List<Entity[]> cliques = new ArrayList<Entity[]>();
        mef.cliqueEnumeration(new CliqueVisitor () {
                public boolean clique (Clique clique) {
                    logger.info("+++++ Clique ("+clique.size()
                                +") found for "+clique.values()+" ++++++");
                    for (Entity e : clique.entities()) {
                        logger.info("  "+e.getId()+": "+e.datasource());
                    }
                    cliques.add(clique.entities());
                    return true;
                }
            });
        assertTrue ("There should have been two cliques found!",
                    cliques.size() == 2);
        
        // calculate connected components
        CurationMetrics metrics = mef.calcCurationMetrics();
        int cc1 = 0;
        Set<DataSource> ds = new HashSet<DataSource>();
        for (Iterator<Entity> it = mef.entities("CC_1"); it.hasNext(); ++cc1) {
            Entity e = it.next();
            //logger.info("node "+e.getId()+" "+e.datasource()+" "+e.datasource().getKey()+" "+e.datasource().getId());
            ds.add(e.datasource());
        }
        logger.info("## "+cc1+" total nodes in CC_1 spanning "+ds);
        assertTrue ("Expecting 3 datasources but instead got "+ds.size(),
                    ds.size() == 3);
        
        mef.shutdown();
    }
}
