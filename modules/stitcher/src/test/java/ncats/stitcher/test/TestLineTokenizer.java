package ncats.stitcher.test;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.LineTokenizer;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

public class TestLineTokenizer {


    @Test
    public void testTerminatingDelimiter () throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ('~');
        try(InputStream is = LineTokenizer.class.getResourceAsStream("/patent.txt")) {
            tokenizer.setInputStream(is);
            int lines = 0;
            for (; tokenizer.hasNext(); ++lines) {
                String[] tokens = tokenizer.next();
                assertEquals("tokens per Line ",9, tokens.length);
            }
            assertEquals("number of lines", 12233, lines);
        }
    }

    @Test
    public void testQuote () throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ();
        try(InputStream is = LineTokenizer.class.getResourceAsStream("/Selleck_Compounds_Partial_List_2015.txt")) {
            tokenizer.setInputStream(is);
            int lines = 0;
            for (; tokenizer.hasNext(); ++lines) {
                String[] tokens = tokenizer.next();
                assertEquals("tokens per Line ", 15, tokens.length);
            }
            assertEquals("number of lines", 2320, lines);
        }
    }
           
}
