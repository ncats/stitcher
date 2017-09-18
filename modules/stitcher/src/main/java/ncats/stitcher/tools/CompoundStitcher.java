package ncats.stitcher.tools;

import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Consumer;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Entity;
import ncats.stitcher.DataSource;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.AuxNodeType;
import ncats.stitcher.CliqueVisitor;
import ncats.stitcher.Clique;
import ncats.stitcher.Props;
import ncats.stitcher.Component;
import ncats.stitcher.Stitch;
import ncats.stitcher.Util;
import ncats.stitcher.UntangleCompoundComponent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class CompoundStitcher implements Consumer<Stitch> {
    static final Logger logger = Logger.getLogger
        (CompoundStitcher.class.getName());

    static class Approval {
        final public boolean approved;
        final public Date date;

        public Approval (boolean approved) {
            this (approved, null);
        }

        public Approval (Date date) {
            this (true, date);
        }

        public Approval (boolean approved, Date date) {
            this.approved = approved;
            this.date = date;
        }
    }
        
    static abstract class ApprovalParser {
        final public String name;
        protected ApprovalParser (String name) {
            this.name = name;
        }

        public abstract Approval getApproval (Map<String, Object> payload);
    }

    static class RanchoApprovalParser extends ApprovalParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
        final SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd");
        Approval approval;

        public RanchoApprovalParser () {
            super ("rancho_2017-09-07_20-32.json");
        }

        void parseApproval (JsonNode n) {
            if (n.has("HighestPhase") && "approved".equalsIgnoreCase
                (n.get("HighestPhase").asText())) {
                if (n.has("ConditionProductDate")) {
                    String d = n.get("ConditionProductDate").asText();
                    try {
                        Date date = sdf.parse(d);
                        if (approval == null || approval.date == null
                            || approval.date.after(date))
                            approval = new Approval (date);
                    }
                    catch (Exception ex) {
                        logger.log(Level.SEVERE,
                                   "Can't parse date: "+d, ex);
                    }
                }
                else {
                    logger.warning("Approved Condition without date!");
                    approval = new Approval (true);
                }
            }
        }

        void parseApproval (String content) throws Exception {
            JsonNode node = mapper.readTree(decoder.decode(content));
            if (node.isArray()) {
                for (int i = 0; i < node.size(); ++i) {
                    JsonNode n = node.get(i);
                    parseApproval (n);
                }
            }
            else 
                parseApproval (node);
        }

        public Approval getApproval (Map<String, Object> payload) {
            approval = null;

            Object content = payload.get("Conditions");
            if (content != null) {
                if (content.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String c = (String) Array.get(content, i);
                        try {
                            parseApproval (c);
                        }
                        catch (Exception ex) {
                            logger.log(Level.SEVERE, 
                                       "Can't parse Json payload: "+c, ex);
                        }
                    }
                }
                else {
                    try {
                        parseApproval ((String)content);
                    }
                    catch (Exception ex) {
                        logger.log(Level.SEVERE, 
                                   "Can't parse json payload: "+content, ex);
                    }
                }
            }

            return approval;
        }
    }
        
    static class NPCApprovalParser extends ApprovalParser {
        public NPCApprovalParser () {
            super ("npc-dump-1.2-04-25-2012_annot.sdf.gz");
        }
        
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object content = payload.get("DATASET");
            if (content != null) {
                if (content.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String s = (String) Array.get(content, i);
                        if (s.toLowerCase().indexOf("approved") >= 0) {
                            approval = new Approval (true);
                            break;
                        }
                    }
                }
                else if (((String)content).toLowerCase().indexOf("approved") >= 0) {
                    approval = new Approval (true);
                }
            }
            
            return approval;
        }
    }
    
    static class PharmManuApprovalParser extends ApprovalParser {
        final ObjectMapper mapper = new ObjectMapper ();
        final Base64.Decoder decoder = Base64.getDecoder();
        final SimpleDateFormat sdf = new SimpleDateFormat ("yyyy");
            
        public PharmManuApprovalParser () {
            super ("PharmManuEncycl3rdEd.json");
        }
            
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
                
            String content = (String) payload.get("Drug Products");
            if (content != null) {
                try {
                    for (String c : content.split("\n")) {
                        JsonNode node = mapper.readTree(decoder.decode(c));
                        if (node.isArray()) {
                            for (int i = 0; i < node.size(); ++i) {
                                JsonNode n = node.get(i);
                                if (n.has("Year Introduced")) {
                                    String year = n.get("Year Introduced").asText();
                                    try {
                                        Date date = sdf.parse(year);
                                        if (approval == null 
                                            || approval.date.after(date))
                                            approval = new Approval (date);
                                    }
                                    catch (Exception ex) {
                                        logger.log(Level.SEVERE, 
                                                   "Can't parse date: "+year, ex);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't parse json: '"+content+"'", ex);
                }
            }
                
            return approval;
        }
    }
        
    static class DrugBankApprovalParser extends ApprovalParser {
        public DrugBankApprovalParser () {
            super ("drugbank-full-annotated.sdf");
        }
            
        public Approval getApproval (Map<String, Object> payload) {
            Approval approval = null;
            Object content = payload.get("DRUG_GROUPS");
            if (content != null) {
                if (content.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(content); ++i) {
                        String s = (String) Array.get(content, i);
                        if (s.toLowerCase().indexOf("approved") >= 0) {
                            approval = new Approval (true);
                            break;
                        }
                    }
                }
                else if (((String)content).toLowerCase().indexOf("approved") >= 0) 
                    approval = new Approval (true);
            }
            return approval;
        }
    }
        
    static ApprovalParser[] ApprovalParsers = {
        new RanchoApprovalParser (),
        new NPCApprovalParser (),
        new PharmManuApprovalParser (),
        new DrugBankApprovalParser ()
    };
        
    final EntityFactory ef;
    final DataSourceFactory dsf;
        
    public CompoundStitcher (String db)  throws Exception {
        ef = new EntityFactory (GraphDb.getInstance(db));
        dsf = ef.getDataSourceFactory();
    }
        
    Approval getApproval (Stitch stitch) {
        Approval approval = null;

        for (ApprovalParser ap : ApprovalParsers) {
            if (stitch.is(ap.name)) {
                Map<String, Object> payload = stitch.payload(ap.name);
                if (payload != null) {
                    Approval a = ap.getApproval(payload);
                    if (a == null) {
                    }
                    else if (approval == null || approval.date == null 
                             || (a.date != null && approval.date.after(a.date))) {
                        logger.info(ap.name+": approved="+a.approved+" date="+a.date);
                        approval = a;
                    }
                }
            }
        }

        return approval;
    }

    /*
     * Consumer<Stitch>
     */
    public void accept (Stitch stitch) {
        Approval approval = getApproval (stitch);
        if (approval != null) {
            logger.info("Stitch "+stitch.getId()+" => approved="+approval.approved+
                        " date="+approval.date);
            stitch.set("approved", approval.approved);
            if (approval.date != null) {
                Calendar cal = Calendar.getInstance();
                cal.setTime(approval.date);
                stitch.set("approvedYear", cal.get(Calendar.YEAR));
            }
            if (approval.approved)
                stitch.addLabel("APPROVED");
        }
    }

    public void shutdown () {
        ef.shutdown();
    }

    public void stitch (int version, Long... components) throws Exception {
        DataSource dsource = dsf.register("stitch_v"+version);
        if (components == null || components.length == 0) {
            // do all components
            logger.info("Untangle all components...");
            List<Long> comps = new ArrayList<>();
            ef.components(component -> {
                    comps.add(component.root().getId());
                });
            logger.info("### "+components.length+" components!");
            for (Long cid : comps) {
                logger.info("########### Untangle component "+cid+"...");
                ef.untangle(new UntangleCompoundComponent
                            (dsource, ef.component(cid)), this);
            }
        }
        else {
            for (Long cid : components) {
                Component comp = ef.component(cid);
                logger.info("Stitching component "+comp.getId());
                ef.untangle(new UntangleCompoundComponent
                            (dsource, comp), this);
            }
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+CompoundStitcher.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }

        CompoundStitcher cs = new CompoundStitcher (argv[0]);
        int version = Integer.parseInt(argv[1]);

        List<Long> comps = new ArrayList<>();
        for (int i = 2; i < argv.length; ++i)
            comps.add(Long.parseLong(argv[i]));
        cs.stitch(version, comps.toArray(new Long[0]));

        cs.shutdown();
    }
}
