package com.clarivate.impl;

import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 03.11.2017.
 */
public class HealthCanadaDrugFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(HealthCanadaDrugFactory.class.getName());

    {
        drugProperties.put("0", "DRUG_CODE");
        drugProperties.put("1", "PRODUCT_CATEGORIZATION");
        drugProperties.put("2", "CLASS");
        drugProperties.put("3", "DRUG_IDENTIFICATION_NUMBER");
        drugProperties.put("4", "PRODUCTS");
        drugProperties.put("5", "DESCRIPTOR");
        drugProperties.put("6", "PEDIATRIC_FLAG");
        drugProperties.put("7", "ACCESSION_NUMBER");
        drugProperties.put("8", "NUMBER_OF_AIS");
        drugProperties.put("9", "LAST_UPDATE_DATE");
        drugProperties.put("10", "AI_GROUP_NO");
        drugProperties.put("11", "CLASS_F");
        drugProperties.put("12", "BRAND_NAME_F");
        drugProperties.put("13", "DESCRIPTOR_F");
    }

    public HealthCanadaDrugFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("DRUG_CODE");
        setUseName(false);

        add(I_Code, "DRUG_CODE")
        .add(E_Label, "Health_Canada_Drug");
    }

    @Override
    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withDelimiter(',');
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + HealthCanadaDrugFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        HealthCanadaDrugFactory hcdf = new HealthCanadaDrugFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting HealthCanada drugs uploading...");
                hcdf.register(arg[i]);
            }
        }
        finally {
            hcdf.shutdown();
        }
    }
}
