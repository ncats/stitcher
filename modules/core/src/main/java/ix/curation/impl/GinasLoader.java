package ix.curation.impl;

import chemaxon.formats.MolFormatException;
import chemaxon.formats.MolImporter;
import chemaxon.marvin.util.MolExportException;
import chemaxon.struc.Molecule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ix.curation.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by southalln on 10/27/2015.
 */
public class GinasLoader extends MoleculeEntityFactory {

    /*
    NAME_DOMAIN drug    Drug
    NAME_DOMAIN cosmetic        Cosmetic
    NAME_DOMAIN food    Food
    NAME_ORG    INN     INN     International Non-proprietary Name
    NAME_ORG    USAN    USAN    United States Adopted Name
    NAME_ORG    INCI    INCI    International Nomenclature of Cosmetic Ingredient
    NAME_ORG    BAN     BAN     British Adopted Name
    NAME_ORG    JAN     JAN     Japanese Adopted Name
    NAME_TYPE   of      Official Name
    NAME_TYPE   sys     Systematic Name
    NAME_TYPE   bn      Brand Name
    NAME_TYPE   cn      Common Name
    NAME_TYPE   cd      Code
     */

    static protected MolImporter _mi = new MolImporter();
    static protected ObjectMapper _mapper = new ObjectMapper();

    static String[] citeSRS = {
            "ALANWOOD.NET",
            "CFSAN",
            "DRUGS@FDA",
            "EMA",
            "EP",
            "EVMPD",
            "FCC",
            "FHFI",
            "GRAS",
            "HPUS",
            "HSDB",
            "ICSAS",
            "JAN",
            "JECFA",
            "MARTINDALE",
            "MERCK INDEX",
            "NDF-RT",
            "ORANGE BOOK",
            "PCPC",
            "SWISS MEDIC",
            "WHO-DD",
            "WIKIPEDIA"
    };


    public GinasLoader(String dir) throws IOException {
        super (dir);
    }

    static public Molecule readJsonRecord(JsonNode record, String unii, String fullUrl)
            throws JsonProcessingException, MolFormatException, MolExportException {

        Molecule cmpd;
        if (record.has("structure")) {
            JsonNode node2 = record.get("structure");
            String structType = "structure:molfile"
                    + ":" + node2.get("stereochemistry").asText()
                    + ":" + node2.get("stereoCenters").asText()
                    + ":" + node2.get("definedStereo").asText()
                    + ":" + node2.get("opticalActivity").asText()
                    + ":" + node2.get("atropisomerism").asText()
                    + ":" + node2.get("stereoComments").asText();
/*
                    stereochemistry: "RACEMIC", ACHIRAL ABSOLUTE MIXED RACEMIC EPIMERIC UNKNOWN
                            opticalActivity: "( + / - )", ( + )  ( - )  none  unspec
                            atropisomerism: "No",
                            stereoComments: "",
                            stereoCenters: 2,
                            definedStereo: 2,
*/
            cmpd = _mi.importMol(node2.get("molfile").asText());

            // add source value that gets computed into racemate
            if (node2.get("stereochemistry").asText().equals("RACEMIC") &&
                    node2.get("definedStereo").asInt() > 0) {
                String molecule = cmpd.exportToFormat("smiles");
                molecule = molecule.replace("@@","||");
                molecule = molecule.replace("@","@@");
                molecule = molecule.replace("||","@");
                cmpd = _mi.importMol(cmpd.exportToFormat("smiles") + "." + molecule);
            }
        } else {
            cmpd = new Molecule();
        }

        cmpd.setProperty(StitchKey.I_UNII.name(), unii);

        String names = "";
        String preferredName = "";
        for (Iterator<JsonNode> j = record.get("names").iterator(); j.hasNext(); ) {
            JsonNode node2 = j.next();
            names += node2.get("name").asText() + "\n";
            if (node2.get("preferred").asBoolean()) preferredName = node2.get("name").asText();
        }
        if (names.length() > 0)
            cmpd.setProperty(StitchKey.N_Name.name(), names);
        if (preferredName.length() > 0)
            cmpd.setProperty("name", preferredName);
        String codes = "";
        for (Iterator<JsonNode> j = record.get("codes").iterator(); j.hasNext(); ) {
            JsonNode node2 = j.next();
            String codeType = "code:";
            if (node2.has("type") && node2.has("codeSystem"))
                codeType += node2.get("type").asText()+":"+node2.get("codeSystem").asText();
            if ("code:PRIMARY:CAS".equals(codeType)) {
                if (cmpd.getProperty(StitchKey.I_CAS.name()) != null)
                    cmpd.setProperty(StitchKey.I_CAS.name(), cmpd.getProperty(StitchKey.I_CAS.name()) + node2.get("code").asText() + "\n");
                else cmpd.setProperty(StitchKey.I_CAS.name(), node2.get("code").asText() + "\n");
            } else if (node2.get("code") != null) {
                codes += node2.get("code").asText() + "\n";
            }
        }
        if (codes.length() > 0)
            cmpd.setProperty(StitchKey.I_Code.name(), codes);


        //"relationships":[{"relatedSubstance":{"substanceClass":"reference","approvalID":"2ZM8CX04RZ","refPname":"INSULIN GLARGINE","refuuid":"cd9541e4-2723-4ab5-ac73-6fcfdbf5c84d"},"type":"SUBSTANCE-\u003eSUB_CONCEPT","references":[],"access":[]}]

        for (Iterator<JsonNode> j = record.get("relationships").iterator(); j.hasNext(); ) {
            JsonNode node2 = j.next();
            String codeType = "related substance";
            if (node2.has("type"))
                codeType = node2.get("type").asText();
            String type = "Related UNII";
            if (codeType.equals("ACTIVE MOIETY"))
                codeType = PredicateType.ActiveMoiety.name();
            // SUBSTANCE->SUB_CONCEPT
            if (codeType.startsWith("SUBSTANCE") && codeType.endsWith("CONCEPT"))
                codeType = PredicateType.ConceptOf.name();
            if (node2.has("relatedSubstance")) {
                JsonNode related = node2.get("relatedSubstance");
                if (related.has("approvalID")) {
                    cmpd.setProperty(codeType, related.get("approvalID").asText());
                }
            }
        }

        if (record.has("mixture") && record.get("mixture").has("components")) {
            StringBuffer comps = new StringBuffer();
            for (Iterator<JsonNode> j = record.get("mixture").get("components").iterator(); j.hasNext(); ) {
                JsonNode node2 = j.next();
                if (node2.has("substance") && node2.get("substance").has("approvalID")) {
                    comps.append(node2.get("substance").get("approvalID").asText() + "\n");
                }
            }
            if (comps.length() > 0)
                cmpd.setProperty(PredicateType.HasComponent.name(), comps.toString());
        }

        // get labels from names and refs
        StringBuffer tags = new StringBuffer();
        tags.append("Class:"+record.get("substanceClass").textValue()+"\n");
        if (record.has("tags")) {
            for (Iterator<JsonNode> j = record.get("tags").iterator(); j.hasNext(); ) {
                tags.append(j.next().textValue() + "\n");
            }
        }
        if (record.has("references")) {
            for (Iterator<JsonNode> j = record.get("references").iterator(); j.hasNext(); ) {
                JsonNode node2 = j.next();
                if (node2.has("citation")) {
                    String cite = node2.get("citation").asText().toUpperCase();
                    if (cite.startsWith("EMEA")) cite = "EMA" + cite.substring(4);
                    if (cite.indexOf(':') == -1 && cite.indexOf('[') == -1) {
                        for (String tag: citeSRS) {
                            if (tag.indexOf(cite) > -1 && tags.indexOf(tag) == -1)
                                tags.append(tag + "\n");
                        }
                    }
                }
            }

        }
        if (tags.length() > 0)
            cmpd.setProperty(StitchKey.T_Keyword.name(), tags.toString());

        return cmpd;
    }

    static GinasLoader load (String[] argv) throws Exception {
        // argv = neo4j.db ginas filename
        String filename = argv[2];
        File file = new File (argv[2]);
        if (!file.exists()) {
            System.err.println("File "+file+" not found!");
            System.exit(1);
        }

        GinasLoader gl = null;
        try {
            gl = new GinasLoader(argv[0]);
            DataSource ds = gl.getDataSourceFactory().register(file);
            ds.setName(argv[1]);
            gl.setDataSource(ds);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        gl.add(StitchKey.I_UNII, StitchKey.I_UNII.name());
        gl.add(StitchKey.N_Name, StitchKey.N_Name.name());
        gl.add(StitchKey.I_CAS, StitchKey.I_CAS.name());
        gl.add(StitchKey.T_Keyword, StitchKey.T_Keyword.name());
        gl.setId(StitchKey.I_UNII.name());
        RelationshipType[] types = {PredicateType.ActiveMoiety, PredicateType.HasComponent, PredicateType.ConceptOf};
        
        HashMap<String, Long> loaded = new HashMap<>();
        try (Transaction tx = gl.gdb.beginTx()) {
            ResourceIterator<Node> i = gl.gdb.findNodes
                (DynamicLabel.label(gl.getDataSource().getName()));
            while (i.hasNext()) {
                Node n = i.next();
                String unii = (String)n.getProperty(Entity.ID);
                loaded.put(unii, n.getId());
            }
        }

        Map<RelationshipType, Map<Long, String>> relatedSubstances = new HashMap<>();
        for (RelationshipType type: types)
            relatedSubstances.put(type, new HashMap<Long, String>());

        String baseUrl = "http://tripod.nih.gov/ginas/app/api/v1/substances";

        BufferedReader br = new BufferedReader(new InputStreamReader
                                               (new GZIPInputStream(new FileInputStream(filename)), "UTF-8"));
        String line = br.readLine();
        int count = 0;
        while (line != null) { // WORKED: 18MXK3D6DB      ab2eaaa2-c135-4c8d-b525-c3e8ea370eda    {"st
            String unii = line.substring(8, line.indexOf('\t'));
            line = line.substring(line.indexOf('\t')+1);
            String uuid = line.substring(0, line.indexOf('\t'));
            String fullUrl = baseUrl+"("+uuid+")?view=full";
            line = line.substring(line.indexOf('\t')+1);

            JsonNode json = _mapper.readTree(line);
            try {
                Molecule mol = readJsonRecord(json, unii, fullUrl);
                System.out.println("+++++ " + (count + 1) + " +++++");
                Long nodeId;
                if (loaded.containsKey(unii)) {
                    System.out.println("Already loaded: "+unii);
                    nodeId = loaded.get(unii);
                    for (RelationshipType type: relatedSubstances.keySet()) {
                        if (mol.getProperty(type.name()) != null
                            && mol.getProperty(type.name()).length() > 0) {
                            for (String nodeName: mol.getProperty(type.name()).split("\n")) {
                                if (!loaded.containsKey(nodeName)) {
                                    relatedSubstances.get(type).put(nodeId, nodeName);
                                }
                            }
                        }
                    }
                } else {
                    try (Transaction tx = gl.gdb.beginTx()) {
                        Entity ent = gl.register(mol);
                        nodeId = ent.getId();
                        loaded.put(unii, nodeId);
                        count++;
                    
                        for (RelationshipType type: relatedSubstances.keySet()) {
                            if (mol.getProperty(type.name()) != null
                                && mol.getProperty(type.name()).length() > 0) {
                                for (String nodeName: mol.getProperty(type.name()).split("\n")) {
                                    if (loaded.containsKey(nodeName)) {
                                        ent._node().createRelationshipTo(
                                                                         gl.gdb.getNodeById(loaded.get(nodeName)), type);
                                    } else {
                                        relatedSubstances.get(type).put(nodeId, nodeName);
                                    }
                                }
                            }
                            if (relatedSubstances.get(type).containsValue(unii)) {
                                Set<Long> keysToDrop = new HashSet<>();
                                for (Long oNodeId : relatedSubstances.get(type).keySet())
                                    if (relatedSubstances.get(type).get(oNodeId).equals(unii)) {
                                        Node oNode = gl.gdb.getNodeById(oNodeId);
                                        oNode.createRelationshipTo(ent._node(), type);
                                        keysToDrop.add(oNodeId);
                                    }
                                for (Long oNodeId : keysToDrop)
                                    relatedSubstances.get(type).remove(oNodeId);
                            }
                        }
                        tx.success();
                    }
                }
            } catch (Exception ex) {ex.printStackTrace();}
            //if (count > 40) break; // TODO
            line = br.readLine();
        }
        System.out.println("records loaded: "+count);

        // link cmpds to active moieties
        count = 0;
        for (RelationshipType type: relatedSubstances.keySet()) {
            for (Long nodeId : relatedSubstances.get(type).keySet()) {
                try (Transaction tx = gl.gdb.beginTx()) {
                    Node node = gl.gdb.getNodeById(nodeId);
                    String unii = relatedSubstances.get(type).get(nodeId);
                    Node oNode = gl.gdb.findNode(DynamicLabel.label(gl.getDataSource().getName()), Entity.ID, unii);
                    if (oNode != null) {
                        node.createRelationshipTo(oNode, type);
                        System.out.println("Entry " + count + " Stitched together active moiety: " + nodeId + ":" + unii + ":" + oNode.toString());
                    } else {
                        System.out.println("Entry " + count + " Failed to stitch together active moiety: " + nodeId + ":" + unii);
                    }
                    count++;
                }
            }
        }
        System.out.println("stitches attempted: "+count);
        return gl;
    }

    static public void main (String[] argv) {
        GinasLoader loader = null;
        try {
            loader = load (argv);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            if (loader != null)
                loader.shutdown();
        }
    }
}
