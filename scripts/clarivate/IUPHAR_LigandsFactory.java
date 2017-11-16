package com.clarivate.impl;

import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 31.10.2017.
 */
public class IUPHAR_LigandsFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(IUPHAR_LigandsFactory.class.getName());

    {
        drugProperties.put("Ligand id", "IUPHAR_ID");
        drugProperties.put("Name", "Common Name");
        drugProperties.put("Species", null);
        drugProperties.put("Type", null);
        drugProperties.put("Approved", null);
        drugProperties.put("Withdrawn", null);
        drugProperties.put("Labelled", null);
        drugProperties.put("Radioactive", null);
        drugProperties.put("PubChem SID", "PUBCHEM");
        drugProperties.put("PubChem CID", "CompoundCID");
        drugProperties.put("UniProt id", "UniProt_ID");
        drugProperties.put("IUPAC name", "Iupac");
        drugProperties.put("INN", "INN");
        drugProperties.put("Synonyms", "SYNONYMS");
        drugProperties.put("SMILES", "SMILES");
        drugProperties.put("InChIKey", null);
        drugProperties.put("InChI", null);
    }


    public IUPHAR_LigandsFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("IUPHAR_ID");
        setUseName(false);

        add(I_IUPHAR, "IUPHAR_ID")
        .add(E_Label, "IUPHAR_Ligands");
    }

    @Override
    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withHeader().withDelimiter(',');
    }

    @Override
    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        if (fieldName.equals("Name")) {
            fieldValue = removeHTMLTags(fieldValue);
            parsedMap.put("Common Name", fieldValue);

            return true;
        }

        if (fieldName.equals("UniProt id")) {
            parsedMap.put("UniProt_ID", splitAndRemoveHTMLTags("\\|", fieldValue));
            return true;
        }

        if (fieldName.equals("Synonyms")) {
            // Parse synonyms
            fieldValue = fieldValue.replaceAll("^\"?", "").replaceAll("\"?$", "");
            parsedMap.put("SYNONYMS", splitAndRemoveHTMLTags("\\|", fieldValue));

            return true;
        }

        return false;
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + IUPHAR_LigandsFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        IUPHAR_LigandsFactory ilf = new IUPHAR_LigandsFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting IUPHAR Ligands uploading...");
                ilf.register(arg[i]);
            }
        }
        finally {
            ilf.shutdown();
        }
    }
}
