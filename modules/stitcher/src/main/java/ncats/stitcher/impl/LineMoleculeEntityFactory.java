package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

import ncats.stitcher.*;

/*
 * parsing of molecules in line format (smiles)
 */
public class LineMoleculeEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(LineMoleculeEntityFactory.class.getName());

    String[] header;
    int count, molcol;
    MolHandler mh = new MolHandler ();
    Set<String> properties = new TreeSet();
    
    public LineMoleculeEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public LineMoleculeEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public LineMoleculeEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    public DataSource register (String name, File file) throws IOException {
        return register (name, file, "\t", -1);
    }

    public DataSource register (String name, File file, String delim) throws IOException {
        return register (name, file, delim, -1);
    }
    
    public DataSource register (String name, File file, String delim, int molcol)
        throws IOException {
        // TODO: make this cleaner...
        this.source = getDataSourceFactory().register(name, file);
        
        Integer instances = (Integer) this.source.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+this.source.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            instances = register (this.source.openStream(), delim, molcol);
            updateMeta (this.source);       
            this.source.set(INSTANCES, instances);
            this.source.set(PROPERTIES, properties.toArray(new String[0]));
            logger.info
                ("$$$ "+instances+" entities registered for "+this.source);
        }
        return this.source;
    }

    void register (int line, String[] row) {
        if (header == null) {
            int pos = row[0].indexOf('#');
            if (pos >= 0)
                row[0] = row[0].substring(pos);
            header = row;
        }
        else {
            if (header.length != row.length) {
                logger.warning("Line "+line
                               +": columns mismatch; "
                               +"expecting "+header.length
                               +" columns but instead got "+row.length);
            }
            int ncols = Math.min(header.length, row.length);
            Map<String, Object> props = new HashMap<String, Object>();
            Molecule mol = null;
            for (int c = 0; c < ncols; ++c) {
                if (row[c] != null && (molcol < 0 || molcol == c)
                    && (strucField == null
                        || (strucField.length() > 0
                            && header[c].equals(strucField)))) {
                    try {
                        if ("NA".equals(row[c].trim())) row[c] = "Not Available"; // Withdrawn file uses NA as not available, but this is a valid smiles
                        mh.setMolecule(row[c]);                         
                        if (molcol < 0) {
                            molcol = c;
                            logger.info("## Column "+(c+1)
                                        +" is designated as mol column!");
                        }
                        mol = mh.getMolecule();
                    }
                    catch (Exception ex) {
                        if (molcol >= 0)
                            logger.warning
                                ("Line "+line+", column "+(c+1)
                                 +": bogus molecule "
                                 +ex.getMessage());
                    }
                }
                props.put(header[c], row[c]);
                properties.add(header[c]);
            }

            Entity e = null;
            if (mol != null) {
                for (Map.Entry<String, Object> me : props.entrySet()) {
                    mol.setProperty(me.getKey(), (String) me.getValue());
                }
                e = register (mol);
            }
            else {
                // otherwise just register the Map..
                e = register (props);
            }
                
            if (e != null)
                ++count;
        }
    }
    
    public int register (InputStream is, String delim, int molcol)
        throws IOException {
        header = null;
        count = 0;
        molcol = -1;
        
        LineTokenizer tokenizer = new LineTokenizer (delim.charAt(0));
        tokenizer.setInputStream(is);
        while (tokenizer.hasNext()) {
            try {
                String[] row = tokenizer.next();
                register (tokenizer.getLineCount(), row);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "can't register at line "
                           +tokenizer.getLineCount(), ex);
            }
        }
        return count;
    }

    public static void main (String[] argv) throws Exception {
        Runnable shutdownHookTask = new ShutdownHookExample();
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHookTask));
        System.out.println("Hook registered!");
        GraphDb.addShutdownHook();
        register (argv, LineMoleculeEntityFactory.class);
    }
}
