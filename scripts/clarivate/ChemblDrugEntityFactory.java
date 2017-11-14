package com.clarivate.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 17.10.2017.
 */
public class ChemblDrugEntityFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(ChemblDrugEntityFactory.class.getName());

    {
        drugProperties.put("PARENT_MOLREGNO", null);
        drugProperties.put("CHEMBL_ID", "ChemblId");
        drugProperties.put("SYNONYMS", "SYNONYMS");
        drugProperties.put("DEVELOPMENT_PHASE", null);
        drugProperties.put("RESEARCH_CODES", null);
        drugProperties.put("APPLICANTS", "Originator");
        drugProperties.put("USAN_STEM", null);
        drugProperties.put("USAN_STEM_DEFINITION", null);
        drugProperties.put("USAN_STEM_SUBSTEM", null);
        drugProperties.put("USAN_YEAR", null);
        drugProperties.put("FIRST_APPROVAL", null);
        drugProperties.put("ATC_CODE", "WHO-ATC");
        drugProperties.put("ATC_CODE_DESCRIPTION", null);
        drugProperties.put("INDICATION_CLASS", null);
        drugProperties.put("SC_PATENT", "CompoundPatent");
        drugProperties.put("DRUG_TYPE", null);
        drugProperties.put("RULE_OF_FIVE", null);
        drugProperties.put("FIRST_IN_CLASS", null);
        drugProperties.put("CHIRALITY", null);
        drugProperties.put("PRODRUG", null);
        drugProperties.put("ORAL", null);
        drugProperties.put("PARENTERAL", null);
        drugProperties.put("TOPICAL", null);
        drugProperties.put("BLACK_BOX", null);
        drugProperties.put("AVAILABILITY_TYPE", null);
        drugProperties.put("WITHDRAWN_YEAR", null);
        drugProperties.put("WITHDRAWN_COUNTRY", null);
        drugProperties.put("WITHDRAWN_REASON", null);
        drugProperties.put("CANONICAL_SMILES", "SMILES");
    }


    public ChemblDrugEntityFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("ChemblId");
        setUseName(false);

        add(I_ChEMBL, "ChemblId")
        .add(E_Label, "Chembl_Drug");
    }

    @Override
    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        if (fieldName.equals("APPLICANTS")) {
            parsedMap.put("Originator", splitAndRemoveHTMLTags(";", fieldValue));
            return true;
        }

        if (fieldName.equals("ATC_CODE")) {
            parsedMap.put("WHO-ATC", splitAndRemoveHTMLTags(";", fieldValue));
            return true;
        }

        if (fieldName.equals("ATC_CODE_DESCRIPTION")) {
            parsedMap.put("ATC_CODE_DESCRIPTION", splitAndRemoveHTMLTags(";", fieldValue));
            return true;
        }

        if (fieldName.equals("INDICATION_CLASS")) {
            parsedMap.put("INDICATION_CLASS", splitAndRemoveHTMLTags(";", fieldValue));
            return true;
        }

        if (fieldName.equals("SYNONYMS")) {
            // Parse synonyms
            String[] synonyms = fieldValue.replaceAll("^\"?", "").replaceAll("\"?$", "")
                                          .split(";");
            List<String> parsedSynonyms = new ArrayList<>();
            for (String synonym : synonyms) {
                synonym = synonym.replaceAll("\\(.*?\\)", "").trim();
                if (!synonym.isEmpty())
                    parsedSynonyms.add(synonym);
            }
            parsedMap.put("SYNONYMS", parsedSynonyms.toArray(new String[parsedSynonyms.size()]));
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + ChemblDrugEntityFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        ChemblDrugEntityFactory cdef = new ChemblDrugEntityFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting Chembl drugs uploading...");
                cdef.register(arg[i]);
            }
        }
        finally {
            cdef.shutdown();
        }
    }
}
