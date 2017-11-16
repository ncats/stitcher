package com.clarivate.impl;

import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 03.11.2017.
 */
public class HealthCanadaIngredFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(HealthCanadaDrugFactory.class.getName());

    {
        drugProperties.put("0", "DRUG_CODE");
        drugProperties.put("1", "ACTIVE_INGREDIENT_CODE");
        drugProperties.put("2", "CompoundName");
        drugProperties.put("3", "INGREDIENT_SUPPLIED_IND");
        drugProperties.put("4", "STRENGTH");
        drugProperties.put("5", "STRENGTH_UNIT");
        drugProperties.put("6", "STRENGTH_TYPE");
        drugProperties.put("7", "DOSAGE_VALUE");
        drugProperties.put("8", "BASE");
        drugProperties.put("9", "DOSAGE_UNIT");
        drugProperties.put("10", "NOTES");
        drugProperties.put("11", "INGREDIENT_FFOOTNOTE");
        drugProperties.put("12", "STRENGTH_UNIT_FFOOTNOTE");
        drugProperties.put("13", "STRENGTH_TYPE_FFOOTNOTE");
        drugProperties.put("14", "DOSAGE_UNIT_FFOOTNOTE");
    }

    public HealthCanadaIngredFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("DRUG_CODE");
        setUseName(false);

        add(I_Code, "DRUG_CODE")
        .add(E_Label, "Health_Canada_Ingred");
    }

    @Override
    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withDelimiter(',');
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + HealthCanadaIngredFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        HealthCanadaIngredFactory hcif = new HealthCanadaIngredFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting HealthCanada Ingred uploading...");
                hcif.register(arg[i]);
            }
        }
        finally {
            hcif.shutdown();
        }
    }
}
