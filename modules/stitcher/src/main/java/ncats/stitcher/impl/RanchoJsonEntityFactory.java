package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.Base64;
import java.net.URL;

import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

public class RanchoJsonEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(RanchoJsonEntityFactory.class.getName());

    int count;
    MolHandler mh = new MolHandler ();
    Base64.Encoder base64 = Base64.getEncoder();
    ObjectMapper mapper = new ObjectMapper ();
    
    public RanchoJsonEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public RanchoJsonEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public RanchoJsonEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("CompoundName");
        setNameField ("CompoundName");
        setUseName (false);
        addBlacklist ("Unknown");
        add (N_Name, "CompoundName")
            .add (N_Name, "CompoundSynonym")
            .add (I_CAS, "Cas")
            //.add (I_UNII, "Unii")
            ;
    }

    @Override
    public int register (InputStream is) throws IOException {
        JsonNode root;
        BufferedInputStream bis = new BufferedInputStream (is, 4096);
        try {
            bis.mark(1024);
            root = mapper.readTree(new GZIPInputStream (bis));
        }
        catch (Exception ex) {
            bis.reset();
            // probably not gzip stream
            root = mapper.readTree(bis);
        }
        
        count = 0;
        int total = root.size();
        for (int i = 0; i < root.size(); ++i) {
            register (root.get(i), total);
        }

        return count;
    }

    void setProperty (Molecule mol, String name, JsonNode node)
        throws Exception {
        if (node.isObject()) {
            mol.setProperty(name, base64.encodeToString
                            (mapper.writeValueAsBytes(node)));
        }
        else if (node.isArray()) {
            StringBuilder buf = new StringBuilder ();
            for (int i = 0; i < node.size(); ++i) {
                JsonNode n = node.get(i);
                String val;
                if (n.isObject()) {
                    val = base64.encodeToString
                        (mapper.writeValueAsBytes(n));
                    /*
                    int len = val.length()-70;
                    StringBuilder sb = new StringBuilder ();
                    int j = 0;
                    for (; j < len; j += 70) {
                        sb.append(val.substring(j, j+70));
                        sb.append('\n');
                    }
                    sb.append(val.substring(j));
                    val = sb.toString();
                    */
                }
                else
                    val = n.asText();
                if (buf.length() > 0) buf.append("\n");
                buf.append(val);
            }
            
            mol.setProperty(name, buf.toString());
        }
        else
            mol.setProperty(name, node.asText());
    }

    void register (JsonNode node, int total) {
        System.out.println("+++++ "+(count+1)+"/"+total+" +++++");
        if (node.has("CompoundSmiles")) {
            String smiles = node.get("CompoundSmiles").asText();
            try {
                mh.setMolecule(smiles);
                Molecule mol = mh.getMolecule();
                for (Iterator<Map.Entry<String, JsonNode>> it = node.fields();
                     it.hasNext(); ) {
                    Map.Entry<String, JsonNode> f = it.next();
                    setProperty (mol, f.getKey(), f.getValue());
                }
                
                //System.out.print(mol.toFormat("sdf"));
                Entity ent = register (mol);
                ++count;
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, node.get("CompoundName").asText()
                           +": Bogus smiles: "+smiles, ex);
            }
        }
        else {
            logger.warning(node.get("CompoundName").asText()
                           +" has no CompoundSmiles field!");
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+RanchoJsonEntityFactory.class.getName()
                               +" DBDIR [cache=DIR] FILE...");
            System.exit(1);
        }

        RanchoJsonEntityFactory mef = new RanchoJsonEntityFactory (argv[0]);
        try {
            for (int i = 1; i < argv.length; ++i) {
                int pos = argv[i].indexOf('=');
                if (pos > 0) {
                    String name = argv[i].substring(0, pos);
                    if (name.equalsIgnoreCase("cache")) {
                        mef.setCache(argv[i].substring(pos+1));
                    }
                    else {
                        logger.warning("** Unknown parameter \""+name+"\"!");
                    }
                }
                else {
                    mef.register(argv[i]);
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }
}
