package com.clarivate.impl;

import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.logging.Logger;

import static ncats.stitcher.StitchKey.*;

/**
 * Created by Clarivate on 03.11.2017.
 */
public class HealthCanadaTherFactory extends AbstractFactory {
    static final Logger logger = Logger.getLogger(HealthCanadaTherFactory.class.getName());

    {
        drugProperties.put("0", "DRUG_CODE");
        drugProperties.put("1", "WHO-ATC");
        drugProperties.put("2", "ATC_DESCRIPTION");
        drugProperties.put("3", "AHFS_CODE");
        drugProperties.put("4", "AHFS_DESCRIPTION");
        drugProperties.put("5", "TC_ATC_FFOOTNOTE");
        drugProperties.put("6", "TC_AHFS_FFOOTNOTE");
    }

    public HealthCanadaTherFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    protected void init() {
        super.init();
        setIdField("DRUG_CODE");
        setUseName(false);

        add(I_Code, "DRUG_CODE")
        .add(E_Label, "Health_Canada_Ther");
    }

    @Override
    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withDelimiter(',');
    }

    public static void main(String[] arg) throws Exception {
        if (arg.length < 2) {
            System.err.println("Usage: " + HealthCanadaTherFactory.class.getName() + " DBDIR FILE...");
            System.exit(1);
        }

        HealthCanadaTherFactory hctf = new HealthCanadaTherFactory(arg[0]);
        try {
            for (int i = 1; i < arg.length; i++) {
                logger.info("Starting HealthCanada Ther uploading...");
                hctf.register(arg[i]);
            }
        }
        finally {
            hctf.shutdown();
        }
    }
}
