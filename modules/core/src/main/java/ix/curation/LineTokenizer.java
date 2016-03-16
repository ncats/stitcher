package ix.curation;

import java.io.*;
import java.util.*;

public class LineTokenizer implements Iterator<String[]> {
    protected char delim;
    protected Reader reader;
    protected char[] buf = new char[1];
    protected String[] tokens;
    
    protected int count, lines;
    protected StringBuilder currentLine = new StringBuilder ();

    public LineTokenizer () {
        this ('\t');
    }

    public LineTokenizer (char delim) {
        this.delim = delim;
    }

    protected String[] nextLine () throws IOException {
        List<String> tokens = new ArrayList<String>();
        int i = 0, nb;

        boolean quote = false;
        StringBuilder tok = new StringBuilder ();       
        while ((nb = reader.read(buf)) != -1) {
            currentLine.append(buf[0]);
            
            if (buf[0] == '"') {
                quote = !quote;
            }
            else if (buf[0] == '\r') {
            }
            else if (buf[0] == '\n') {
                ++lines;
                if (!quote) {
                    tokens.add(tok.length() > 0? tok.toString() : null);
                    break;
                }
                else
                    tok.append('\n');
            }
            else if (buf[0] != delim || quote) {
                tok.append(buf[0]);
            }
            else {
                tokens.add(tok.length() > 0 ? tok.toString() : null);
                tok.setLength(0);
            }
        }

        if (nb == -1) {
            if (tokens.isEmpty()) return null;
            // in case no terminating newline at the end of the last record
            if (buf[0] != '\n')
                tokens.add(tok.length() > 0 ? tok.toString() : null);           
        }
        ++count;
        
        return tokens.toArray(new String[0]);
    }
    
    public void setInputStream (InputStream is) throws IOException {
        reader = new BufferedReader (new InputStreamReader (is));
        lines = 0;
        count = 0;
        currentLine.setLength(0);
        tokens = nextLine ();
    }

    public int getCount () { return count; }
    public int getLineCount () { return lines; }
    public String getCurrentLine () {
        return currentLine.toString();
    }
    
    public boolean hasNext () {
        return tokens != null;
    }

    public String[] next () {
        if (tokens == null)
            throw new IllegalStateException ("No line available");
        String[] line = tokens;
        try {
            tokens = nextLine ();
        }
        catch (IOException ex) {
            ex.printStackTrace();
            tokens = null;
        }
        return line;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: LineTokenizer FILES...");
            System.exit(1);
        }

        LineTokenizer tokenizer = new LineTokenizer ();
        for (String a : argv) {
            System.out.println("["+a+"]");
            tokenizer.setInputStream(new FileInputStream (a));
            for (int i = 0; tokenizer.hasNext(); ++i) {
                String[] tokens = tokenizer.next();
                System.out.print(i+": ("+tokens.length+")");
                for (int j = 0; j< tokens.length; ++j)
                    System.out.print(" <<"+tokens[j]+">>");
                System.out.println("--\n");
            }
        }
    }
}
