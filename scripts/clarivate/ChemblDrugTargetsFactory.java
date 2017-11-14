package com.clarivate.impl;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 23.10.2017.
 */
public class ChemblDrugTargetsFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(ChemblDrugTargetsFactory.class.getName());

    {
        drugProperties.put("MOLECULE_CHEMBL_ID", "ChemblId");
        drugProperties.put("MOLECULE_NAME", "Chemical Name");
        drugProperties.put("MOLECULE_TYPE", null);
        drugProperties.put("FIRST_APPROVAL", null);
        drugProperties.put("ATC_CODE", null);
        drugProperties.put("ATC_CODE_DESCRIPTION", null);
        drugProperties.put("USAN_STEM", null);
        drugProperties.put("MECHANISM_OF_ACTION", null);
        drugProperties.put("MECHANISM_COMMENT", null);
        drugProperties.put("SELECTIVITY_COMMENT", null);
        drugProperties.put("TARGET_CHEMBL_ID", null);
        drugProperties.put("TARGET_NAME", "TARGETS");
        drugProperties.put("ACTION_TYPE", "CompoundMOA");
        drugProperties.put("ORGANISM", "NCBI TAXONOMY");
        drugProperties.put("TARGET_TYPE", null);
        drugProperties.put("SITE_NAME", null);
        drugProperties.put("BINDING_SITE_COMMENT", null);
        drugProperties.put("MECHANISM_REFS", "References");
        drugProperties.put("CANONICAL_SMILES", null);
    }

    public ChemblDrugTargetsFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("ChemblId");
        setUseName(false);

        add(I_ChEMBL, "ChemblId")
        .add(E_Label, "Chembl_Drug_Target");
    }

    @Override
    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        if (fieldName.equals("MECHANISM_REFS")) {
            // parse mechanism refs
            String[] chunks = fieldValue.split("\\|");
            for (String chunk : chunks) {
                String[] commaChunks = chunk.split(",");
                for (String commaChunk : commaChunks) {
                    if (commaChunk.trim().toLowerCase().startsWith("url:http")) {
                        parsedMap.put("References", commaChunk.trim().substring(4));
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + ChemblDrugTargetsFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        ChemblDrugTargetsFactory cdtf = new ChemblDrugTargetsFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting Chembl Drug Targets uploading...");
                cdtf.register(arg[i]);
            }
        }
        finally {
            cdtf.shutdown();
        }
    }
}
