package utils;

import java.util.*;
import java.util.regex.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Util {
    public static JsonNode toJsonNode (String...values) {
        ObjectMapper mapper = new ObjectMapper ();
        ArrayNode array = mapper.createArrayNode();
        if (values != null) {
            for (String v : values) 
                array.add(v);
        }
        return array;
    }

    public static String[] tokenizer (String line, char delim) {
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
        // line ends in the delim character, so we add one more value
        else if (line.charAt(len-1) == delim)
            toks.add(null);

        return toks.toArray(new String[0]);
    }

    static Pattern FloatRegex =
        Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
    static Pattern IntegerRegex = Pattern.compile("^[-+]?[0-9]+$");
    
    public static Class typeInference (String token) {
        Matcher m = IntegerRegex.matcher(token);
        if (m.matches())
            return Long.class;

        m = FloatRegex.matcher(token);
        if (m.matches())
            return Double.class;

        // ok, give up
        return String.class;
    }
}
