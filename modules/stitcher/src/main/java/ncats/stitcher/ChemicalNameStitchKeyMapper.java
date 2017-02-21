package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChemicalNameStitchKeyMapper implements StitchKeyMapper {
    static final Logger logger = Logger.getLogger
        (ChemicalNameStitchKeyMapper.class.getName());
    
    static {
        try {
            InputStream is = ChemicalNameStitchKeyMapper.class
                .getResourceAsStream("/salt-names.txt");
            BufferedReader br = new BufferedReader
                (new InputStreamReader (is));
            for (String line; (line = br.readLine()) != null; ) {
                line = line.trim().toUpperCase();
                int pos = line.indexOf('*');
                if (pos == 0) { // suffix matching
                }
                else if (pos == line.length() - 1) { // prefix matching
                }
                else if (pos != -1) {
                }
                else {
                    
                }
            }
            br.close();
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE,
                       "Can't load resource file salt-names.txt!", ex);
        }
    }

    public ChemicalNameStitchKeyMapper () {
    }

    @Override
    public Map<StitchKey, Object> map (Object value) {
        Map<StitchKey, Object> values = new HashMap<>();
        if (value != null) {
            
        }
        return values;
    }

    public static void main (String[] argv) throws Exception {
        ChemicalNameStitchKeyMapper chemNameMapper =
            new ChemicalNameStitchKeyMapper ();
    }
}
