package com.clarivate.impl;

import ncats.stitcher.Entity;
import ncats.stitcher.impl.MoleculeEntityFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Clarivate on 27.10.2017.
 */
public abstract class AbstractFactory extends MoleculeEntityFactory {
    static final Logger logger = Logger.getLogger(ChemblDrugTargetsFactory.class.getName());

    protected int count = 0;
    protected CSVParser parser;
    
    protected Map<String, String> drugProperties = new HashMap<>();

    public AbstractFactory(String dir) throws IOException {
        super(dir);
    }

    @Override
    public int register(InputStream is) throws IOException {
        count = 0;
        Reader reader = new InputStreamReader(is);
        parser = new CSVParser(reader, getCSVFormat());

        int ln = 1;
        try {
            for (CSVRecord record : parser) {
                try {
                    register(record, ln++);
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, "can't register entry: " + record.toString(), ex);
                }
            }
        } finally {
            reader.close();
            is.close();
        }

        return count;
    }

    protected void register(CSVRecord record, int total) {
        logger.info("+++++ " + (count + 1) + "/" + total + " +++++");

        Map<String, Object> map = new HashMap();

        for (Map.Entry<String, String> entry : drugProperties.entrySet()) {
            String key = entry.getKey();
            String stitchKey = entry.getValue();
            if (stitchKey == null) {
                stitchKey = key.toUpperCase().replaceAll(" ", "_");
            }

            try {
                String value;
                Map headerMap = parser.getHeaderMap();
                if (headerMap != null)
                    value = record.get(key).trim();
                else
                    value = record.get(Integer.parseInt(key)).trim();
                if (value != null) {
                    if (!value.isEmpty() && !handleSpecifiedField(key, value, map)) {
                        map.put(stitchKey, value);
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.info("Skipping " + key + " for line #" + (count + 1));
            }
        }

        properties.addAll(map.keySet());

        Entity ent = register(map);

        count++;
    }

    protected CSVFormat getCSVFormat() {
        return CSVFormat.EXCEL.withHeader().withDelimiter('\t');
    }

    protected boolean handleSpecifiedField(String fieldName, String fieldValue, Map<String, Object> parsedMap) {
        return false;
    }

    protected String removeHTMLTags(String value) {
        Pattern p = Pattern.compile("(<.*?>)");
        Matcher m = p.matcher(value);

        while(m.find()) {
            String startTag = m.group(1);
            String endTag = startTag.replace("<", "</");
            value = value.replaceFirst(startTag, "");
            value = value.replaceFirst(endTag, "");
            m = p.matcher(value);
        }

        return value;
    }

    protected String[] splitAndRemoveHTMLTags(String delimiter, String value) {
        String[] values = value.split(delimiter);
        List<String> parsedValues = new ArrayList<>();
        for (String v : values) {
            v = v.trim();
            if (!v.isEmpty()) {
                v = removeHTMLTags(v);
                parsedValues.add(v);
            }
        }

        return parsedValues.toArray(new String[parsedValues.size()]);
    }
}
