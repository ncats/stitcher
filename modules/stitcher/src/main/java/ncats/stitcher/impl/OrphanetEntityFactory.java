package ncats.stitcher.impl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;


/*
 * base class for orphanet data
 */
public abstract class OrphanetEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(OrphanetEntityFactory.class.getName());

    static final String NS = "";
    static final QName DisorderList = new QName (NS, "DisorderList");
    static final QName Disorder = new QName (NS, "Disorder");
    static final QName DisorderGroup = new QName (NS, "DisorderGroup");
    static final QName ExpertLink = new QName (NS, "ExpertLink");
    static final QName DisorderType = new QName (NS, "DisorderType");
    static final QName OrphaNumber = new QName (NS, "OrphaNumber");
    static final QName OrphaCode = new QName (NS, "OrphaCode");
    static final QName Name = new QName (NS, "Name");

    static class Term {
        public Integer orphaNumber;
        public String name;        
    }
    
    static class Disorder extends Term {
        public List<Disorder> parents = new ArrayList<>();
        public String type; // DisorderType
        public String link; // ExperLink
        public List<Entity> entities = new ArrayList<>();
        public List<Disorder> children = new ArrayList<>();

        void add (Disorder d) {
            d.parents.add(this);
            children.add(d);
        }
    }
    
    public OrphanetEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetEntityFactory (File dir) throws IOException {
        super (dir);
    }

    protected Entity[] getEntities (Term t) {
        return getEntities (t, "ORPHA");
    }
    
    protected Entity[] getEntities (Term t, String prefix) {
        List<Entity> entities = new ArrayList<>();
        Iterator<Entity> iter = find ("notation", prefix+":"+t.orphaNumber);
        while (iter.hasNext()) {
            Entity e = iter.next();
            entities.add(e);
        }
        return entities.toArray(new Entity[0]);
    }
    
    protected Entity[] getEntities (Disorder d) {
        if (d.entities.isEmpty()) {
            Map<String, Object> r = new LinkedHashMap<>();
            if (source != null)
                r.put(SOURCE, source.getKey());
            
            Iterator<Entity> iter = find ("notation", "ORPHA:"+d.orphaNumber);
            while (iter.hasNext()) {
                Entity e = iter.next();
                if (!d.parents.isEmpty()) {
                    for (Disorder p : d.parents) {
                        Entity[] pes = getEntities (p);
                        if (pes.length > 0) {
                            for (Entity z : pes) {
                                e.stitch(z, R_subClassOf,
                                         "ORPHA:"+p.orphaNumber, r);
                                //System.out.println(d.entity.getId()+" -> "+p.getId());
                            }
                        }
                    }
                }
                d.entities.add(e);
            }
        }
        else {
            logger.warning("Unable to find entity for ORPHA:"+d.orphaNumber
                           +" "+d.name);
        }
        return d.entities.toArray(new Entity[0]);
    }

    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null || count == 0) {
            setDataSource (ds);
            try (InputStream is = new FileInputStream (file)) {
                count = register (is);
                ds.set(INSTANCES, count);
                updateMeta (ds);
            }
            catch (Exception ex) {
                throw new IOException (ex);
            }
        }
        else {
            logger.info("### Data source "+ds.getName()+" ("+ds.getKey()+") "
                        +"is already registered with "+count+" entities!");
        }
        return ds;        
    }

    protected abstract int register (InputStream is) throws Exception;
}
