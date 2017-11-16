package com.clarivate.impl;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 23.10.2017.
 */
public class ChemblIndicationsFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(ChemblDrugEntityFactory.class.getName());

    {
        drugProperties.put("MOLECULE_CHEMBL_ID", "ChemblId");
        drugProperties.put("MOLECULE_NAME", "Chemical Name");
        drugProperties.put("MOLECULE_TYPE", null);
        drugProperties.put("FIRST_APPROVAL", null);
        drugProperties.put("MESH_ID", null);
        drugProperties.put("MESH_HEADING", "MESH");
        drugProperties.put("EFO_ID", null);
        drugProperties.put("EFO_NAME", null);
        drugProperties.put("MAX_PHASE_FOR_IND", null);
        drugProperties.put("USAN_YEAR", null);
        drugProperties.put("REFS", "References");
    }

    public ChemblIndicationsFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("ChemblId");
        setUseName(false);

        add(I_ChEMBL, "ChemblId")
        .add(E_Label, "Chembl_Indication");
    }

    @Override
    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        if (fieldName.equals("REFS")) {
            // Parse REFS
            String[] chunks = fieldValue.split("\\$\\$");
            for (String chunk : chunks) {
                if (chunk.trim().toLowerCase().startsWith("http")) {
                    parsedMap.put("References", chunk.trim());
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + ChemblIndicationsFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        ChemblIndicationsFactory cif = new ChemblIndicationsFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting Chembl Indications uploading...");
                cif.register(arg[i]);
            }
        }
        finally {
            cif.shutdown();
        }
    }
}
