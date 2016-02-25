package ix.curation.impl;

import ix.curation.*;
import ix.curation.graph.GraphEditor;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Created by southalln on 12/10/15.
 */
public class MakeABigDump {
    static final Logger logger = Logger.getLogger
            (GraphEditor.class.getName());

    public static void main (String[] argv) throws Exception {

        String neoDB = "curator.ix/data.db";
        String ginas = "ginas";
        String ginasfile = "../inxight-planning/files/public2015-11-30.gsrs";
        String npc = "npc-dump";
        String npcFile = "../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz";
        String integr = "integr";
        String integrFile = "../inxight-planning/files/integr.sdf.gz";
        String iuphar = "iuphar";
        String iupharFile = "../inxight-planning/files/iuphar_lig_targets.sdf";
        String drugbank = "drugbank";
        String drugbankFile = "../inxight-planning/files/DrugBank_targets.sdf";
        String chembl = "chembl20";
        String chemblURL = "jdbc:mysql://pauli.ncats.nih.gov/chembl_20?user=chembl_20&password=chembl_20";
        String ncgc = "ncgc";
        String ncgcFile = "../inxight-planning/files/NCGC_lib.txt.gz";

        if (argv.length != 0) {
            logger.info("Usage: "+MakeABigDump.class.getName()+" Whoa! Do this by hand you dummy!");
            System.exit(1);
        }

        if (new File(neoDB).exists()) {
            logger.info("Usage: "+MakeABigDump.class.getName()+" Whoa! Delete the neo DB first!");
            System.exit(1);
        }

        String[] main = {neoDB, ginas, ginasfile};
        GinasLoader.main(main); // load first set of compounds -- if we try to load too much we run out of memory
        GinasLoader.main(main); // load rest of SRS compounds

        GraphDatabaseService gdb;

        // set connected component labels
        gdb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(neoDB);
        try {
            RelationshipType[] keys = {StitchKey.I_UNII, StitchKey.I_CAS, StitchKey.N_Name,
                    StitchKey.H_LyChI_L4, StitchKey.H_LyChI_L5, PredicateType.ActiveMoiety, PredicateType.ConceptOf};
            EntityType[] types = {EntityType.Agent};
            CurationMetrics metrics = MoleculeEntityFactory.calcCurationMetrics(gdb, types, keys);
            logger.info("CurationMetric: " + Util.toJson(metrics));
        }
        finally {
            gdb.shutdown();
        }

        gdb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(neoDB);
        try {
            GraphEditor ge = new GraphEditor(gdb);

            ge.deprecateMetaLabel(DynamicLabel.label(ginas), StitchKey.I_CAS, true, 0, false);
            ge.deprecateMetaLabel(DynamicLabel.label(ginas), StitchKey.N_Name, true, 2, false);
            ge.deprecateMetaLabel(DynamicLabel.label(ginas), StitchKey.H_LyChI_L4, false, 7, false);

        }
        finally {
            gdb.shutdown();
        }

        String[] main2 = {neoDB, npc, npcFile, "I_CAS:CAS", "N_Name:Synonyms", "I_UNII:CompoundUNII", "T_Keyword:DATASET", "id:ID"};
        MoleculeEntityFactory.main(main2);

        String[] main3 = {neoDB, integr, integrFile, "I_CAS:CAS", "N_Name:Drug_Name", "T_Keyword:Highest_Phase", "id:Prous_Science_Entry_Number"};
        MoleculeEntityFactory.main(main3);

        String[] main4 = {neoDB, iuphar, iupharFile, "N_Name:ligand", "N_Name:INN", "I_UniProt:I_UniProt", "id:ligand_id"};
        MoleculeEntityFactory.main(main4);

        String[] main5 = {neoDB, drugbank, drugbankFile, "N_Name:GENERIC_NAME", "I_UniProt:I_UniProt", "id:DATABASE_ID"};
        MoleculeEntityFactory.main(main5);

        String[] main6 = {neoDB, chembl, chemblURL, "I_UniProt:I_UniProt", "I_ChEMBL:I_ChEMBL", "N_Name:GENERIC_NAME", "id:chembl_id"};
        MoleculeEntityFactory.main(main6);

        gdb = new GraphDatabaseFactory().newEmbeddedDatabase(neoDB);
        try {
            GraphEditor ge = new GraphEditor(gdb);
            RelationshipType[] conservativeKeys = {StitchKey.I_UNII, StitchKey.H_LyChI_L5, StitchKey.N_Name, StitchKey.H_LyChI_L4, StitchKey.I_CAS};
            ArrayList<String> rts = new ArrayList<>();
            for (RelationshipType rt: conservativeKeys)
                rts.add(rt.name());

            //ge.chooseClosestSource(EntityType.Agent, DynamicLabel.label(ginas), rts);
            ge.greedyConnectedComponents(DynamicLabel.label(ginas), rts);
            ge.writeOutNodeTags("NodesAndLabels.txt");

        }
        finally {
            //gdb.shutdown();
        }

        String[] main7 = {neoDB, ncgc, ncgcFile};
        NCGCLoader.main(main7);

    }

}
