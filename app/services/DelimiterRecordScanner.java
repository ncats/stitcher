package services;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import play.Logger;
import ix.curation.Util;

public class DelimiterRecordScanner extends RecordScanner<String[]> {
    protected String delimiter;
    protected BufferedReader reader;

    public static class CSV extends DelimiterRecordScanner {
        public CSV () {
            super (",");
        }
    }

    public static class TXT extends DelimiterRecordScanner {
        public TXT () {
            super ("\t");
        }
    }

    public static class TSV extends DelimiterRecordScanner {
        public TSV () {
            super ("\t");
        }
    }
    
    public DelimiterRecordScanner (String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void setInputStream (InputStream is) throws Exception {
        reset ();
        reader = new BufferedReader (new InputStreamReader (is));
        String head = reader.readLine();
        properties = parseLine (head);
    }

    protected String[] parseLine (String line) {
        return delimiter.length() == 1
            ? Util.tokenizer (line, delimiter.charAt(0))
            : line.split(delimiter);
    }

    @Override
    protected String[] getProperties (String[] record) {
        List<String> props = new ArrayList<String>();   
        if (record.length != properties.length) {
            Logger.warn("Line "+(count+1)+": mismatch fields; expecting "
                        +properties.length+" columns but instead got "
                        +record.length);
        }
        else {
            for (int i = 0; i < properties.length; ++i) {
                if (record[i] == null || record[i].length() == 0) {
                }
                else {
                    Class old = types.get(properties[i]);
                    Class cls = Util.typeInference(record[i]);
                    if (old == null
                        || (cls == Double.class && old == Long.class)
                        || cls == String.class) {
                        types.put(properties[i], cls);
                    }
                    props.add(properties[i]);
                }
            }
        }
        
        return props.toArray(new String[0]);
    }
    
    @Override
    protected String[] getNext () throws Exception {
        return parseLine (reader.readLine());
    }
}
