package com.clarivate.impl;

import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.E_Label;
import static ncats.stitcher.StitchKey.I_IUPHAR;

/**
 * Created by Clarivate on 31.10.2017.
 */
public class IUPHAR_InteractionsFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(IUPHAR_InteractionsFactory.class.getName());

    {
        drugProperties.put("target", "TARGETS");
        drugProperties.put("target_id", null);
        drugProperties.put("target_gene_symbol", null);
        drugProperties.put("target_uniprot", "UniProt_ID");
        drugProperties.put("target_ligand", "TARGETS");
        drugProperties.put("target_ligand_id", null);
        drugProperties.put("target_ligand_gene_symbol", null);
        drugProperties.put("target_ligand_uniprot", "UniProt_ID");
        drugProperties.put("target_ligand_pubchem_sid", null);
        drugProperties.put("target_species", null);
        drugProperties.put("ligand", null);
        drugProperties.put("ligand_id", "IUPHAR_ID");
        drugProperties.put("ligand_gene_symbol", null);
        drugProperties.put("ligand_species", null);
        drugProperties.put("ligand_pubchem_sid", null);
        drugProperties.put("type", null);
        drugProperties.put("action", "CompoundMOA");
        drugProperties.put("action_comment", null);
        drugProperties.put("endogenous", null);
        drugProperties.put("primary_target", null);
        drugProperties.put("concentration_range", null);
        drugProperties.put("affinity_units", null);
        drugProperties.put("affinity_high", null);
        drugProperties.put("affinity_median", null);
        drugProperties.put("affinity_low", null);
        drugProperties.put("original_affinity_units", null);
        drugProperties.put("original_affinity_low_nm", null);
        drugProperties.put("original_affinity_median_nm", null);
        drugProperties.put("original_affinity_high_nm", null);
        drugProperties.put("original_affinity_relation", null);
        drugProperties.put("assay_description", null);
        drugProperties.put("receptor_site", null);
        drugProperties.put("ligand_context", null);
        drugProperties.put("pubmed_id", null);
    }

    public IUPHAR_InteractionsFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("IUPHAR_ID");
        setUseName(false);

        add(I_IUPHAR, "IUPHAR_ID")
        .add(E_Label, "IUPHAR_Interactions");
    }

    @Override
    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        if (fieldName.equals("target") || fieldName.equals("target_ligand")) {
            fieldValue = removeHTMLTags(fieldValue);
            parsedMap.put("TARGETS", fieldValue);

            return true;
        }

        if (fieldName.equals("target_ligand_id")) {
            parsedMap.put("TARGET_ID", fieldValue);

            return true;
        }

        if (fieldName.equals("target_gene_symbol") || fieldName.equals("target_ligand_gene_symbol")) {
            parsedMap.put("TARGET_GENE_SYMBOL", splitAndRemoveHTMLTags("\\|", fieldValue));

            return true;
        }

        if (fieldName.equals("target_uniprot") || fieldName.equals("target_ligand_uniprot")) {
            parsedMap.put("UniProt_ID", splitAndRemoveHTMLTags("\\|", fieldValue));

            return true;
        }

        return false;
    }

    @Override
    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withHeader().withDelimiter(',');
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + IUPHAR_InteractionsFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        IUPHAR_InteractionsFactory iif = new IUPHAR_InteractionsFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting IUPHAR Interactions uploading...");
                iif.register(arg[i]);
            }
        }
        finally {
            iif.shutdown();
        }
    }
}
