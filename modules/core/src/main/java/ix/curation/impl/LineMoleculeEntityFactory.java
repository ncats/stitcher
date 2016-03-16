package ix.curation.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

import ix.curation.*;

/*
 * parsing of molecules in line format (smiles)
 */
public class LineMoleculeEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(LineMoleculeEntityFactory.class.getName());

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
    public DataSource register (File file) throws IOException {
        return register (file, "\t", -1);
    }

    public DataSource register (File file, String delim) throws IOException {
        return register (file, delim, -1);
    }
    
    public DataSource register (File file, String delim, int molcol)
        throws IOException {
        // TODO: make this cleaner...
        this.source = getDataSourceFactory().register(file);
        
        Integer instances = (Integer) this.source.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+this.source.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            instances = register (this.source.openStream(), delim, molcol);
            this.source.set(INSTANCES, instances);
            updateMeta (this.source);
            logger.info("$$$ "+instances+" entities registered for "+this.source);
        }
        return this.source;
    }

    public int register (InputStream is, String delim, int molcol)
        throws IOException {
        String[] header = null;
        MolHandler mh = new MolHandler ();

        int count = 0; // entity count
        LineTokenizer tokenizer = new LineTokenizer (delim.charAt(0));
        tokenizer.setInputStream(is);
        while (tokenizer.hasNext()) {
            String[] row = tokenizer.next();

            if (header == null) {
                int pos = row[0].indexOf('#');
                if (pos >= 0)
                    row[0] = row[0].substring(pos);
                header = row;
            }
            else {
                if (header.length != row.length) {
                    logger.warning("Line "+tokenizer.getLineCount()
                                   +": columns mismatch; "
                                   +"expecting "+header.length
                                   +" columns but instead got "+row.length
                                   +"\n"+tokenizer.getCurrentLine());
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
                                    ("Line "+tokenizer.getLineCount()
                                     +", column "+(c+1)
                                     +": bogus molecule "
                                     +ex.getMessage());
                        }
                    }
                    props.put(header[c], row[c]); 
                }

                Entity e = null;
                if (mol != null) {
                    for (Map.Entry<String, Object> me : props.entrySet())
                        mol.setProperty(me.getKey(), (String)me.getValue());
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
        return count;
    }

    public static void main (String[] argv) throws Exception {
        GraphDb.addShutdownHook();
        register (argv, LineMoleculeEntityFactory.class);
    }
}
