package services;

import java.io.*;

public class DelimiterRecordReader extends RecordReader {
    protected String delimiter;
    protected BufferedReader reader;
    protected String[] header;

    public DelimiterRecordReader () {
        this (",");
    }
    
    public DelimiterRecordReader (String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void setInputStream (InputStream is) throws Exception {
        reader = new BufferedReader (new InputStreamReader (is));
        count = 0;
        current = getNext ();
    }

    protected Object parseLine (String line) {
        return line; // do nothing
    }

    @Override
    protected Object getNext () throws Exception {
        return parseLine (reader.readLine());
    }
}
