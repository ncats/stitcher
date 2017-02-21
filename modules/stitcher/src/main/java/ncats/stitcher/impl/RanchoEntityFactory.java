package ncats.stitcher.impl;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;
import ncats.stitcher.Entity;
import ncats.stitcher.GraphDb;
import ncats.stitcher.LineTokenizer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by southalln on 2/19/17.
 */
public class RanchoEntityFactory extends LineMoleculeEntityFactory {
    public RanchoEntityFactory(String dir) throws IOException {
        super(dir);
    }

    public RanchoEntityFactory(File dir) throws IOException {
        super(dir);
    }

    public RanchoEntityFactory(GraphDb graphDb) throws IOException {
        super(graphDb);
    }

    protected Entity register (MolHandler mh, String[] header, String[] row, Map<String, Object> props, int molcol, int lineCount) {
        Molecule mol = null;
        for (int c = 0; c < row.length; ++c) {
            if (row[c] != null && (molcol < 0 || molcol == c)
                    && (strucField == null
                    || (strucField.length() > 0
                    && header[c].equals(strucField)))) {
                try {
                    mh.setMolecule(row[c]);
                    if (molcol < 0) {
                        molcol = c;
                        logger.info("## Column " + (c + 1)
                                + " is designated as mol column!");
                    }
                    mol = mh.getMolecule();
                } catch (Exception ex) {
                    if (molcol >= 0)
                        logger.warning
                                ("Line " + lineCount
                                        + ", column " + (c + 1)
                                        + ": bogus molecule "
                                        + ex.getMessage());
                }
            }
        }

        Entity e = null;
        if (mol != null) {
            for (Map.Entry<String, Object> me : props.entrySet())
                mol.setProperty(me.getKey(), (String) me.getValue());
            e = register(mol);
        } else {
            // otherwise just register the Map..
            e = register(props);
        }

        return e;
    }

    @Override
    public int register (InputStream is, String delim, int molcol)
            throws IOException {
        String[] header = null;
        MolHandler mh = new MolHandler ();
        int drugCols = 39;
        int condCols = 23;
        int targetCols = 15;
        String[] prevRow = {""};
        Map<String, Object> props = new HashMap<>();
        int count = 0;

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
                            +" columns but instead got "+row.length);
                }
                int ncols = Math.min(header.length, row.length);

                if (row[0].equals(prevRow[0])) {
                    // figure out what is new
/*
                    for (int c=0; c<drugCols; c++)
                        if (!row[c].equals(prevRow[c]))
                            logger.warning("Line "+tokenizer.getLineCount()
                            +" mismatch in compound information");
*/
                    if (row[drugCols] != null &&
                            !row[drugCols].equals(prevRow[drugCols]) &&
                            !row[drugCols].equals("Unknown")) {
                        // add a condition
                        for (int c=drugCols; c<drugCols+condCols; c++) {
                            props.put(header[c], props.get(header[c])+"|"+row[c]);
                        }
                    }
                    if (row[drugCols+condCols] != null &&
                            !row[drugCols+condCols].equals(prevRow[drugCols+condCols]) &&
                            !row[drugCols+condCols].equals("Unknown")) {
                        // add a target
                        for (int c=drugCols+condCols; c<drugCols+condCols+targetCols; c++) {
                            props.put(header[c], props.get(header[c])+"|"+row[c]);
                        }
                    }
                } else {
                    // register previous row
                    Entity e = register(mh, header, prevRow, props, molcol, tokenizer.getLineCount());
                    if (e != null)
                        ++count;

                    props = new HashMap<>();
                    for (int c = 0; c < ncols; ++c) {
                        if (row[c] != null)
                            props.put(header[c], row[c]);
                    }
                }
                prevRow = row;
            }
        }

        register(mh, header, prevRow, props, molcol, tokenizer.getLineCount());
        return count;
    }

    public static void main (String[] argv) throws Exception {
        GraphDb.addShutdownHook();
        register (argv, RanchoEntityFactory.class);
        //System.exit(0);

        /*
        String[] gsrsArgv = {"base.db", "data/public2016-06-10.gsrs"};
        SRSJsonEntityFactory.main(gsrsArgv);
        String[] fdaArgv = {"base.db", "data/drugsatfda.zip"};
        DrugsAtFDA.main(fdaArgv);
        String[] ranchoArgv = {"base.db", "data/rancho.conf"};
        register (ranchoArgv, RanchoEntityFactory.class);
        */
    }
}
