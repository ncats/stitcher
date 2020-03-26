package ncats.stitcher.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XmlStream extends FilterInputStream {
    static final Logger logger = Logger.getLogger(XmlStream.class.getName());
    static final byte[] TAG_XML = "<?xml version=\"1.0\"?>\n".getBytes();
    
    ByteArrayOutputStream buf = new ByteArrayOutputStream (1024);
    int start, stop, count;
    BiConsumer<XmlStream, byte[]> consumer;
    boolean done;

    byte[] startTag;
    byte[] endTag;

    public XmlStream (InputStream is) {
        this (is, null);
    }
    
    public XmlStream (InputStream is, String tag) {
        this (is, tag, null);
    }
    
    public XmlStream (InputStream is, String tag,
                      BiConsumer<XmlStream, byte[]> consumer) {
        super (is);
        if (tag != null)
            setTag (tag);
        this.consumer = consumer;
        clear ();
    }

    public void setTag (String tag) {
        if (tag == null || tag.length() == 0)
            throw new IllegalArgumentException ("Tag must not be empty!");
        tag = tag.replaceAll("(<|>)", "");
        startTag = ("<"+tag+">").getBytes();
        endTag = ("</"+tag+">").getBytes();
    }
    
    void clear () {
        start = 0;
        stop = 0;
        buf.reset();
        try {
            buf.write(TAG_XML);
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, "Can't write xml", ex);
        }
    }
    
    void publish () {
        if (buf.size() > TAG_XML.length) {
            byte[] xml = buf.toByteArray();
            if (consumer != null)
                consumer.accept(this, xml);
            ++count;
        }
    }
    
    void add (byte b) {
        boolean intag = start < startTag.length
            && (b == startTag[start]
                || (start+1 == startTag.length && b == ' '));
        if (start == startTag.length || intag) {
            buf.write(b);
            if (stop < endTag.length && b == endTag[stop]) {
                if (++stop == endTag.length) {
                    publish ();
                    clear ();
                }
            }
            else stop = 0;
            
            if (intag)
                ++start;
        }
        else {
            clear ();
        }
    }

    protected boolean isDone () {
        if (startTag == null || startTag.length == 0
            || endTag == null || endTag.length == 0)
            throw new RuntimeException ("No tag specified!");
        return done;
    }
    public void setDone (boolean done) { this.done = done; }
    
    public int read () throws IOException {
        if (isDone ()) return -1;
        int ch = super.read();
        if (ch != -1) {
            byte b = (byte)(ch & 0xff);
            add (b);
        }
        else {
            publish ();
        }
        return ch;
    }

    public int read (byte[] b) throws IOException {
        if (isDone ()) return -1;
        int nb = super.read(b);
        if (nb != -1) {
            for (int i = 0; i < nb; ++i)
                add (b[i]);
        }
        else {
            publish ();
        }
                
        return nb;
    }
        
    public int read (byte[] b, int off, int len) throws IOException {
        if (isDone ()) return -1;
        int nb = super.read(b, off, len);
        if (nb != -1) {
            for (int i = 0; i < nb; ++i)
                add (b[off+i]);
        }
        else {
            publish ();
        }
        return nb;
    }

    public int getCount () { return count; }
    public int start () throws IOException {
        count = 0;
        byte[] buf = new byte[1024];
        for (int nb; (nb = read (buf, 0, buf.length)) != -1; )
            ;
        
        return count;
    }

    // sbt stitcher/"runMain ncats.stitcher.impl.XmlStream FILE.XML.GZ TAG"
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: XmlStream FILE.XML.GZ TAG");
            System.exit(1);
        }

        try (XmlStream xs = new XmlStream
             (new GZIPInputStream (new FileInputStream (argv[0])), argv[1],
              (_xs, xml) -> {
                 System.out.println("--- "+_xs.getCount()+"\n"
                                    +new String (xml));
              })) {
            int count = xs.start();
            logger.info(count+" record(s) streamed!");
        }
    }
}
