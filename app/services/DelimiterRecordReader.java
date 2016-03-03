package services;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import play.Logger;


public class DelimiterRecordReader extends RecordReader<String[]> {
    protected String delimiter;
    protected BufferedReader reader;
    protected String[] header;

    public static class CSV extends DelimiterRecordReader {
        public CSV () {
            super (",");
        }
    }

    public static class TXT extends DelimiterRecordReader {
        public TXT () {
            super ("\t");
        }
    }

    public static class TSV extends DelimiterRecordReader {
        public TSV () {
            super ("\t");
        }
    }
    
    public DelimiterRecordReader (String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void setInputStream (InputStream is) throws Exception {
        reader = new BufferedReader (new InputStreamReader (is));
        count = 0;

        String head = reader.readLine();
        header = parseLine (head);
        current = getNext ();
    }

    protected String[] parseLine (String line) {
        return delimiter.length() == 1
            ? tokenizer (line, delimiter.charAt(0)) : line.split(delimiter);
    }

    @Override
    protected String[] getProperties (String[] record) {
        List<String> props = new ArrayList<String>();   
        if (record.length != header.length) {
            Logger.warn("Line "+(count+1)+": mismatch fields; expecting "
                        +header.length+" columns but instead got "
                        +record.length);
        }
        else {
            for (int i = 0; i < header.length; ++i) {
                if (record[i] == null || record[i].length() == 0) {
                }
                else {
                    props.add(header[i]);
                }
            }
        }
        
        return props.toArray(new String[0]);
    }
    
    @Override
    protected String[] getNext () throws Exception {
        return parseLine (reader.readLine());
    }

    static String[] tokenizer (String line, char delim) {
        List<String> toks = new ArrayList<String>();

        int len = line.length(), parity = 0;
        StringBuilder curtok = new StringBuilder ();
        for (int i = 0; i < len; ++i) {
            char ch = line.charAt(i);
            if (ch == '"') {
                parity ^= 1;
            }
            if (ch == delim) {
                if (parity == 0) {
                    String tok = null;
                    if (curtok.length() > 0) {
                        tok = curtok.toString();
                    }
                    toks.add(tok);
                    curtok.setLength(0);
                }
                else {
                    curtok.append(ch);
                }
            }
            else if (ch != '"') {
                curtok.append(ch);
            }
        }

        if (curtok.length() > 0) {
            toks.add(curtok.toString());
        }

        return toks.toArray(new String[0]);
    }
}
