package ncats.stitcher.impl;

import chemaxon.formats.MolImporter;
import chemaxon.struc.Molecule;
import ncats.stitcher.DataSource;
import ncats.stitcher.Entity;
import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.graph.GraphEditor;
import org.neo4j.graphdb.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by southalln on 12/20/15.
 */
public class NCGCLoader extends MoleculeEntityFactory {

    static MolImporter _mi = new MolImporter();

    public NCGCLoader(String dir) throws IOException {
        super(dir);
    }

/*
    select smiles_iso, sample_id, supplier, supplier_id, ins_date, sample_name,
    pubchem_sid, primary_moa, gene_symbol, gene_id, vendor_info, library, purity_rating
    from REGISTRY.NCGC_SAMPLE
    where Supplier not in ('DPISMR', 'MLSMR', 'Ligand')
    and sample_id not like 'MLS%'
*/

    // #|SMILES_ISO|SAMPLE_ID|SUPPLIER|SUPPLIER_ID|INS_DATE|SAMPLE_NAME|PUBCHEM_SID|PRIMARY_MOA|GENE_SYMBOL|GENE_ID|VENDOR_INFO|LIBRARY|PURITY_RATING
    // 0   1          2         3         4          5         6           7          8           9           10      11          12       13
    Molecule readRecord(String[] sline, String[] header) {
        Molecule mol = new Molecule();
        if (sline[1].length() > 0)
            try {
                mol = _mi.importMol(sline[1]);
            } catch (Exception e) {e.printStackTrace();}
        mol.setProperty("name", sline[2]); // id field for NCGC loader
        mol.setName(sline[2]); // loaded checks this
        String name = sline[2];
        if (sline.length > 6 && sline[6].length() > 0)
            name = name + "\n" + sline[6];
        mol.setProperty(StitchKey.N_Name.name(), name);

        for (int i=3; i<header.length; i++) {
            if (sline.length > i && sline[i].length() > 0)
                mol.setProperty(header[i], sline[i]);
        }

        return mol;
    }

    static String[] split (String line, Character delimit) {
        ArrayList<String> sline = new ArrayList<>();
        int start = 0;
        for (int end=0; end<line.length(); end++)
            if (line.charAt(end) == delimit) {
                sline.add(line.substring(start,end));
                start = end+1;
            }
        sline.add(line.substring(start,line.length()));
        for (int i=0; i<sline.size(); i++)
            if (sline.get(i).length() > 1 && sline.get(i).charAt(0) == '"' && sline.get(i).endsWith("\""))
                sline.set(i, sline.get(i).substring(1,sline.get(i).length()-1));
        return sline.toArray(new String[0]);
    }

    public static void main (String[] argv) {
        // argv = neo4j.db ncgc filename
        String neoDB = argv[0];
        String filename = argv[2];

        NCGCLoader nl = null;
        try {
            nl = new NCGCLoader(neoDB);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Transaction tx = nl.gdb.beginTx();
            nl.add(StitchKey.N_Name, StitchKey.N_Name.name());
            nl.setIdField("name");
            //nl.add(StitchKey.T_Keyword, StitchKey.T_Keyword.name());

            try {
                DataSource ds = nl.register(argv[1]);
                nl.setDataSource(ds);
                HashMap<String, Long> loaded = new HashMap<>();
                ResourceIterator<Node> i = nl.gdb.findNodes(DynamicLabel.label(ds.getName()));
                while (i.hasNext()) {
                    Node n = i.next();
                    String ncgc = (String) n.getProperty(Entity.ID);
                    loaded.put(ncgc, n.getId());
                }

                BufferedReader br = new BufferedReader(new InputStreamReader
                        (new GZIPInputStream(new FileInputStream(filename)), "UTF-8"));
                Character delimiter = '|';
                String[] header = split(br.readLine(), delimiter);
                String line = br.readLine();
                int count = 0;
                while (line != null) {
                    String[] sline = split(line, delimiter);
                    while (sline.length < header.length) { // handle new lines and carriage returns in a line
                        line = line.trim() + br.readLine();
                        sline = split(line, delimiter);
                    }
                    Molecule mol = nl.readRecord(sline, header);
                    System.out.println("+++++ " + (count + 1) + " +++++");
                    if (loaded.containsKey(mol.getName())) {
                        System.out.println("Already loaded: "+mol.getName());
                    } else {
                        Entity ent = nl._register(mol);
                        count++;
                    }
                    if (count > 10000) {
                        count = 0;
                        tx.success();
                        nl.gdb.shutdown();
                        nl = new NCGCLoader(neoDB);
                        tx = nl.gdb.beginTx();
                    }
                    line = br.readLine();
                }
                System.out.println("records loaded: "+count);
                br.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            tx.success();
        }
        finally {
            //nl.graphDb.shutdown();
        }

        try {
            GraphDb graphDb = GraphDb.getInstance(neoDB);
            GraphEditor ge = new GraphEditor(graphDb.graphDb());
            RelationshipType[] conservativeKeys = {StitchKey.I_UNII, StitchKey.H_LyChI_L5, StitchKey.N_Name, StitchKey.H_LyChI_L4, StitchKey.I_CAS};
            ArrayList<String> rts = new ArrayList<>();
            for (RelationshipType rt: conservativeKeys)
                rts.add(rt.name());

            ge.greedyConnectedComponents(DynamicLabel.label("ginas"), rts);
            ge.writeOutNodeTags("NodesAndLabels.txt");
            graphDb.shutdown();
        } catch (Exception e) {e.printStackTrace();}


    }

    static public class FDANameNormalizer {
        static final String transforms=".ALPHA. \u03b1\n" +
                ".BETA. \u03b2\n" +
                ".GAMMA.        \u03b3\n" +
                ".DELTA.        \u03b4\n" +
                ".EPSILON.      \u03b5\n" +
                ".ZETA. \u03b6\n" +
                ".ETA.  \u03b7\n" +
                ".THETA.        \u03b8\n" +
                ".IOTA. \u03b9\n" +
                ".KAPPA.        \u03ba\n" +
                ".LAMBDA.       \u03bb\n" +
                ".MU.   \u03bc\n" +
                ".NU.   \u03bd\n" +
                ".XI.   \u03be\n" +
                ".OMICRON.      \u03bf\n" +
                ".PI.   \u03c0\n" +
                ".RHO.  \u03c1\n" +
                ".SIGMA.        \u03c3\n" +
                ".TAU.  \u03c4\n" +
                ".UPSILON.      \u03c5\n" +
                ".PHI.  \u03c6\n" +
                ".CHI.  \u03c7\n" +
                ".PSI.  \u03c8\n" +
                ".OMEGA.        \u03c9\n" +
                "+/-    \u00b1\n";
        static List<String> transformList = new ArrayList<String>();
        static{
            for(String line:transforms.split("\n")){
                transformList.add(line);
            }
        }

        public static String toFDA(String name){
            for(String trans:transformList){
                name=name.replace(trans.split("\t")[1], trans.split("\t")[0]);
            }
            name=name.toUpperCase().trim();
            return name;
        }
        public static String fromFDA(String name){
            for(String trans:transformList){
                name=name.replace(trans.split("\t")[0], trans.split("\t")[1]);
            }
            return name;
        }

    }
}
