package ix.curation.test;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ix.curation.LineTokenizer;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

public class TestLineTokenizer {
    static final Logger logger =
        Logger.getLogger(TestLineTokenizer.class.getName());

    @Rule public TestName name = new TestName();


    public TestLineTokenizer () {
    }

    @Test
    public void testTerminatingDelimiter () throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ('~');
        
        tokenizer.setInputStream
            (LineTokenizer.class.getResourceAsStream("/patent.txt"));
        int lines = 0;
        for (; tokenizer.hasNext(); ++lines) {
            String[] tokens = tokenizer.next();
            assertTrue ("Line "+lines+": expecting 9 tokens but instead got "
                        +tokens.length, tokens.length == 9);
        }
        assertTrue
            ("Expecting there to be 12233 lines but instead got "+lines,
             lines == 12233);
    }

    @Test
    public void testQuote () throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ();
        tokenizer.setInputStream
            (LineTokenizer.class.getResourceAsStream
             ("/Selleck_Compounds_Partial_List_2015.txt"));
        int lines = 0;
        for (; tokenizer.hasNext(); ++lines) {
            String[] tokens = tokenizer.next();
            assertTrue ("Line "+lines+": expecting 15 tokens but instead got "
                        +tokens.length, tokens.length == 15);
        }
        assertTrue
            ("Expecting there to be 2320 lines but instead got "+lines,
             lines == 2320);
    }
           
}
