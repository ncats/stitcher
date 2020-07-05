package ncats.stitcher;

import java.io.*;
import java.util.*;

public class LineTokenizer implements Iterator<String[]> {
    protected char delim;
    protected Reader reader;
    protected char[] buf = new char[1];
    protected String[] tokens;
    protected boolean checkQuote = true;
    
    protected int count, lines;
    protected StringBuilder currentLine = new StringBuilder ();

    public LineTokenizer () {
        this ('\t');
    }

    public LineTokenizer (char delim) {
        setDelimiter (delim);
    }

    public void setDelimiter (char delim) {
        this.delim = delim;
    }
    public char getDelimiter () { return delim; }
    public void setCheckQuote (boolean checkQuote) {
        this.checkQuote = checkQuote;
    }

    protected String[] nextLine () throws IOException {
        List<String> tokens = new ArrayList<String>();
        int i = 0, nb;

        boolean quote = false;
        StringBuilder tok = new StringBuilder ();
        currentLine.setLength(0);
        while ((nb = reader.read(buf)) != -1) {
            currentLine.append(buf[0]);
            
            if (buf[0] == '"' && checkQuote) {
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
            System.err.println("Usage: LineTokenizer [delimiter=tab] FILES...");
            System.exit(1);
        }

        LineTokenizer tokenizer = new LineTokenizer ();
        Map<String, Set<String>> uvals = new HashMap<>();       
        for (String a : argv) {
            int pos = a.indexOf('=');
            if (pos > 0) {
                if ("delimiter".equals(a.substring(0, pos))) {
                    String delimiter = a.substring(pos+1);
                    if ("tab".equalsIgnoreCase(delimiter))
                        delimiter = "\t";
                    System.err.println("DELIMITER="+delimiter.charAt(0));
                    tokenizer.setDelimiter(delimiter.charAt(0));
                }
            }
            else {
                System.out.println("["+a+"]");
                tokenizer.setInputStream(new FileInputStream (a));
                String[] header = null;
                for (int i = 0; tokenizer.hasNext(); ++i) {
                    String[] tokens = tokenizer.next();
                    //System.out.print(i+": ("+tokens.length+")");
                    if (header == null) {
                        for (int j = 0; j< tokens.length; ++j)
                            System.out.print(" <<"+tokens[j]+">>");
                        header = tokens;
                    }
                    else {
                        for (int j = 0; j< tokens.length; ++j) {
                            /*
                            System.out.print
                            (" <<"+header[j]+">:<"+tokens[j]+">>");*/
                            Set<String> uv = uvals.get(header[j]);
                            if (uv == null)
                                uvals.put(header[j], uv = new TreeSet<>());
                            if (tokens[j] != null)
                                uv.add(tokens[j]);
                        }
                    }
                    //System.out.println("--\n");
                }
            }
        }

        System.out.println("##### unique values ######");
        for (Map.Entry<String, Set<String>> me : uvals.entrySet()) {
            System.out.println("++++++ "+me.getKey()+" +++++++");
            for (String v : me.getValue())
                System.out.println(v);
        }
    }
}
