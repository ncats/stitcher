package services;

import java.io.InputStream;
import java.util.*;

import play.Logger;
import chemaxon.formats.MolImporter;
import chemaxon.struc.Molecule;

import ix.curation.Util;

public class MolRecordScanner extends RecordScanner<Molecule> {
    protected MolImporter mi;
    
    public MolRecordScanner () {
    }

    public void setInputStream (InputStream is) throws Exception {
        mi = new MolImporter (is);
        reset ();
    }

    @Override
    protected String[] getProperties (Molecule mol) {
        List<String> props = new ArrayList<String>();
        for (int i = 0; i < mol.getPropertyCount(); ++i) {
            String p = mol.getPropertyKey(i);
            String v = mol.getProperty(p);
            if (v != null && v.length() > 0) {
                Class old = types.get(p);
                Class cls = Util.typeInference(v);
                if (old == null
                    || (cls == Double.class && old == Long.class)
                    || cls == String.class) {
                    types.put(p, cls);
                }
                props.add(p);
            }
        }
        return props.toArray(new String[0]);
    }

    @Override
    protected Molecule getNext () throws Exception {
        return mi.read();
    }
}
